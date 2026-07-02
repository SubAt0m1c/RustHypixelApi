use std::{fs, marker::PhantomData, path::PathBuf, sync::Arc, time::Duration};

use bytes::Bytes;
use concurrent_slotmap::{Key, SlotId};
use flume::Sender;
use futures::{StreamExt, stream::FuturesUnordered};
use papaya::{HashMap, LocalGuard};

use crate::{Result, bucket::{ActivePartition, Bucket, BucketRef}, error::Error, expiration_queue::{ExpCMD, spawn_expiration_task}, partition::{Partition, PartitionEntry, PartitionMap}, runtime::{Runtime, SendRuntime}, unix_secs};

#[derive(Clone, Copy, PartialEq, Eq)]
pub struct ParKey(SlotId);
impl ParKey {
    #[inline]
    pub fn new(index: u32, generation: u32) -> Self {
        Self(SlotId::new(index, generation))
    }

    #[inline]
    pub fn data(&self) -> (u32, u32) {
        (self.0.index(), self.0.generation())
    }
}

impl Key for ParKey {
    #[inline]
    fn from_id(value: SlotId) -> Self {
        Self(value)
    }

    #[inline]
    fn as_id(self) -> SlotId {
        self.0
    }
}

#[derive(Clone, Copy)]
pub(crate) struct CacheEntry {
    partition_key: ParKey,
    position: PartitionEntry,
}

/// Lifetime managed database.
/// Async down to file io (handled by input runtime)
/// Deletions are batched by a windowed time (default 1 minute)
#[derive(Clone)]
pub struct Database<RT: Runtime + Send + Sync + 'static> {
    inner: Arc<DbInner<RT>>
}

/// hashmaps for these are accessed via _sync variants because locks never need to be held 
/// across awaits, so the overhead will likely cost more than the operation holding the lock.
/// 
/// while scc recomends against this, tokio's docs recommend this in their async mutex/rwlock
/// docs. Going with tokio here since it makes sense and scc doesn't give a reason.
pub(super) struct DbInner<RT: SendRuntime> {
    pub(super) partitions: PartitionMap,
    pub(super) entries: HashMap<u128, CacheEntry>,
    buckets: HashMap<u64, Bucket>,
    exp_tx: Sender<ExpCMD>,
    path: PathBuf,
    _phantom: PhantomData<RT>
}

impl<RT: SendRuntime> DbInner<RT> {
    pub(super) fn new(exp_tx: Sender<ExpCMD>, path: impl Into<PathBuf>) -> Self {
        Self {
            buckets: HashMap::new(),
            entries: HashMap::new(),
            partitions: PartitionMap::new(180),
            exp_tx,
            path: path.into(),
            _phantom: PhantomData
        }
    }
}

impl<RT: Runtime + Send + Sync + 'static> Database<RT> {
    /// Loads a database from a directory.
    pub async fn load(path: &'static str) -> Result<Self> {
        let (exp_tx, rx) = flume::unbounded::<ExpCMD>();

        let inner: Arc<DbInner<RT>> = Arc::new(DbInner::new(exp_tx, path));

        let mut bucket_futures = FuturesUnordered::new();
        for entry in RT::spawn_blocking(move || {
            fs::create_dir_all(path)?;
            fs::read_dir(path)
        }).await?? {
            let entry = entry?;
            if !entry.file_type()?.is_dir() { continue }

            let Some(bucket_id) = entry.file_name().into_string().ok().and_then(|n| n.parse::<u64>().ok()) else { continue };
            let bucket_path = entry.path();
            let bucket_ttl = Duration::from_millis(bucket_id);
            let mut active = ActivePartition::new(ParKey::from_id(SlotId::INVALID), 0);

            let inner = inner.clone();
            bucket_futures.push(async move {
                let mut partition_futures = FuturesUnordered::new();

                for entry in RT::spawn_blocking(move || fs::read_dir(bucket_path)).await?? {
                    let entry = entry?;
                    if !entry.file_type()?.is_file() { continue }
                    let Some(insertion) = entry.file_name().into_string().ok().and_then(|n| n.parse::<u64>().ok()) else { continue };

                    partition_futures.push(async move {
                        (insertion, RT::spawn_blocking(move || Partition::from_file(entry.path())).await.flatten())
                    });
                }
                
                while let Some((insertion, res)) = partition_futures.next().await {
                    let (keys, partition) = res?;
                    
                    let par_key = inner.partitions.insert(partition, &inner.partitions.pin());
                    inner.exp_tx.send(ExpCMD::Schedule { time: insertion + bucket_ttl.as_secs(), par_key }).map_err(Error::queue)?;

                    active = ActivePartition::new(par_key, insertion);  // the last file should be the most recent, and thus the last active one.
                    
                    let guard = inner.entries.guard();
                    for (key, entry) in keys {
                        let cache_entry = CacheEntry { partition_key: par_key, position: entry, };
                        let _ = inner.entries.insert(key, cache_entry, &guard);
                    }
                }

                let _ = inner.buckets.insert(bucket_id, Bucket::new_existing(active, bucket_ttl, entry.path()), &inner.buckets.guard());
                Ok::<_, Error>(())
            });
        }

        while let Some(res) = bucket_futures.next().await { res?; }

        spawn_expiration_task::<RT>(inner.clone(), rx);
        Ok(Self { inner })
    }

    /// Creates a new database without care for previous target directory contents.
    pub fn create_new(path: impl Into<PathBuf>) -> Self {
        let (exp_tx, rx) = flume::unbounded::<ExpCMD>();
        let inner = Arc::new(DbInner {
            buckets: HashMap::new(),
            entries: HashMap::new(),
            partitions: PartitionMap::new(180),
            exp_tx,
            path: path.into(),
            _phantom: PhantomData
        });

        spawn_expiration_task::<RT>(inner.clone(), rx);
        
        Self { inner }
    }

    /// inserts an item into the database with a given ttl.
    pub async fn insert(&self, key: u128, value: impl Into<Bytes>, ttl: Duration) -> Result<()> {
        let now = unix_secs();
        let value = value.into();
        let cache_id = ttl.as_secs();

        let guard = self.get_or_create_bucket(cache_id, ttl, now).await?;

        let bucket_ref = BucketRef::new(cache_id, &self.inner.buckets);
        let (partition_key, position) = bucket_ref.insert::<RT>(guard, key, value, &self.inner.partitions, &self.inner.exp_tx).await?;
        let _ = self.inner.entries.insert(key, CacheEntry { partition_key, position }, &self.inner.entries.guard());
        Ok(())
    }

    /// Attempts to get an entry from the database.
    /// Returns Ok(None) if the entry isn't in the database.
    pub async fn read(&self, key: u128) -> Result<Option<Bytes>> {
        let Some(CacheEntry { partition_key, position }) = self.inner.entries.get(&key, &self.inner.entries.guard()).map(|e|  *e) else {
            return Ok(None)
        };

        self.inner.partitions.get_ref(partition_key).read::<RT>(position).await.map(|b| Some(b))
    }

    async fn get_or_create_bucket(&self, id: u64, ttl: Duration, now: u64) -> Result<LocalGuard<'_>> {
        let guard = self.inner.buckets.guard();
        
        if self.inner.buckets.contains_key(&id, &guard) {
            return Ok(guard)
        }

        drop(guard);

        let path = self.inner.path.join(format!("{}", ttl.as_millis()));
        let bucket = Bucket::new::<RT>(path, now, ttl, &self.inner.partitions, &self.inner.exp_tx).await?;
        
        let guard = self.inner.buckets.guard();
        self.inner.buckets.insert(id, bucket, &guard);
        Ok(guard)
    }
}
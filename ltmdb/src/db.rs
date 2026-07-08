use std::{fs, marker::PhantomData, path::PathBuf, sync::Arc, time::Duration};

use bytes::Bytes;
use concurrent_slotmap::{Key, SlotId};
use flume::Sender;
use futures_util::{StreamExt, stream::FuturesUnordered};
use papaya::{HashMap, LocalGuard};

use crate::{Result, bucket::{ActivePartition, Bucket, BucketRef}, error::Error, expiration_queue::{ExpCMD, run_expiration_task}, hasher::RapidHash, partition::{Partition, PartitionEntry, PartitionMap}, runtime::{Runtime, SendRuntime}, sized_bytes::SizedBytes, unix_secs};

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct ParKey(SlotId);
impl ParKey {
    #[inline]
    pub fn new(index: u32, generation: u32) -> Self {
        let key = if generation & SlotId::STATE_MASK == SlotId::OCCUPIED_TAG {
            // SAFETY: Just checked that generation is occupied
            unsafe { SlotId::new_unchecked(index, generation) } 
        } else {
            SlotId::INVALID
        };
        
        Self(key)
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

/// Lifetime managed key-value store.
/// Async down to file io (handled by input runtime)
/// Expirations are delegated to a background expiration task and batched by a 1 minute window
#[derive(Clone)]
pub struct Database<RT: Runtime + Send + Sync + 'static> {
    inner: Arc<DbInner<RT>>
}

pub(super) struct DbInner<RT: SendRuntime> {
    pub(super) partitions: PartitionMap,
    pub(super) entries: HashMap<SizedBytes, CacheEntry, RapidHash>,
    buckets: HashMap<u64, Bucket, RapidHash>,
    exp_tx: Sender<ExpCMD>,
    path: PathBuf,
    _phantom: PhantomData<RT>
}

impl<RT: SendRuntime> DbInner<RT> {
    pub(super) fn new(exp_tx: Sender<ExpCMD>, path: impl Into<PathBuf>) -> Self {
        Self {
            buckets: HashMap::with_hasher(RapidHash::default()),
            entries: HashMap::with_hasher(RapidHash::default()),
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

            let Some(bucket_millis) = entry.file_name().into_string().ok().and_then(|n| n.parse::<u64>().ok()) else { continue };
            let bucket_path = entry.path();
            let bucket_ttl = Duration::from_millis(bucket_millis);
            let bucket_id = bucket_ttl.as_secs();
            let mut last_insertion = 0;
            let mut last_par_key = ParKey::from_id(SlotId::INVALID);

            let inner = inner.clone();
            bucket_futures.push(async move {
                let mut partition_futures = FuturesUnordered::new();

                for entry in RT::spawn_blocking(move || fs::read_dir(bucket_path)).await?? {
                    let entry = entry?;
                    if !entry.file_type()?.is_file() { continue }
                    let Some(insert_time) = entry.file_name().into_string().ok().and_then(|n| n.parse::<u64>().ok()) else { continue };

                    partition_futures.push(async move {
                        let partition_res = RT::spawn_blocking(move || Partition::from_file(entry.path())).await.flatten();
                        (insert_time, partition_res)
                    });
                }
                
                while let Some((insert_time, partition_res)) = partition_futures.next().await {
                    let (keys, partition) = partition_res?;
                    
                    let par_key = inner.partitions.insert(partition, &inner.partitions.pin());
                    inner.exp_tx.send(ExpCMD::Schedule { time: insert_time + bucket_id, par_key }).map_err(Error::queue)?;

                    if insert_time > last_insertion {
                        last_insertion = insert_time;
                        last_par_key = par_key;
                    }
                    
                    let guard = inner.entries.guard();
                    for (key, entry) in keys {
                        let cache_entry = CacheEntry { partition_key: par_key, position: entry, };
                        let _ = inner.entries.insert(key, cache_entry, &guard);
                    }
                }

                let active = ActivePartition::new(last_par_key, last_insertion);
                let bucket = Bucket::new_existing(active, bucket_ttl, entry.path());
                inner.buckets.pin().insert(bucket_id, bucket);
                Ok::<_, Error>(())
            });
        }

        while let Some(res) = bucket_futures.next().await {
            if let Err(e) = res { return Err(e) }
        }
        
        RT::spawn(run_expiration_task::<RT>(inner.clone(), rx));
        Ok(Self { inner })
    }

    /// Creates a new database without care for previous target directory contents.
    pub fn create_new(path: impl Into<PathBuf>) -> Self {
        let (exp_tx, rx) = flume::unbounded::<ExpCMD>();
        let inner = Arc::new(DbInner {
            buckets: HashMap::with_hasher(RapidHash::default()),
            entries: HashMap::with_hasher(RapidHash::default()),
            partitions: PartitionMap::new(180),
            exp_tx,
            path: path.into(),
            _phantom: PhantomData
        });

        RT::spawn(run_expiration_task::<RT>(inner.clone(), rx));
        
        Self { inner }
    }

    /// inserts an item into the database with a given ttl.
    pub async fn insert(&self, key: impl Into<SizedBytes>, value: impl Into<Bytes>, ttl: Duration) -> Result<()> {
        let now = unix_secs();
        let value = value.into();
        let cache_id = ttl.as_secs();

        let guard = self.get_or_create_bucket(cache_id, ttl, now).await?;

        let bucket_ref = BucketRef::new(cache_id, &self.inner.buckets);
        let key = key.into();
        let (partition_key, position) = bucket_ref.insert::<RT>(guard, key.clone(), value, &self.inner.partitions, &self.inner.exp_tx).await?;
        let _ = self.inner.entries.pin().insert(key, CacheEntry { partition_key, position });
        Ok(())
    }

    /// Attempts to get an entry from the database.
    /// Returns Ok(None) if the entry isn't in the database.
    pub async fn read(&self, key: impl Into<SizedBytes>) -> Result<Option<Bytes>> {
        let Some(CacheEntry { partition_key, position }) = self.inner.entries.pin().get(&key.into()).map(|e|  *e) else {
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
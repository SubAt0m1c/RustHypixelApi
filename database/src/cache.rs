use std::{fs, marker::PhantomData, path::PathBuf, sync::Arc, time::Duration};

use bytes::Bytes;
use concurrent_slotmap::{hyaline::Guard, Key, SlotId, SlotMap};
use dashmap::DashMap;
use flume::Sender;

use crate::{bucket::{ActivePartition, Bucket}, error::Error, expiration_queue::{spawn_expiration_task, ExpCMD}, partition::{Partition, PartitionEntry}, runtime::{Runtime, SendRuntime}, unix_secs, Result};

#[derive(Clone, Copy, PartialEq, Eq)]
pub struct ParKey(SlotId);
impl ParKey {
    #[inline]
    pub fn encode(&self) -> u64 {
        u64::from(self.0.index()) | (u64::from(self.0.generation()) << 32)
    }

    #[inline]
    pub fn decode(encoded: u64) -> Self {
        Self(SlotId::new(encoded as u32, (encoded >> 32) as u32))
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

/// Memory mapped, managed ttl database.
/// Async down to file io (handled by input runtime)
/// Deletions are batched by a windowed time (default 1 minute)
#[derive(Clone)]
pub struct Database<RT: Runtime + Send + Sync + 'static> {
    inner: Arc<DbInner<RT>>
}

pub(super) struct DbInner<RT: SendRuntime> {
    pub(super) partitions: SlotMap<ParKey, Partition>,
    pub(super) entries: DashMap<u128, CacheEntry>,
    buckets: DashMap<u64, Bucket>,
    exp_tx: Sender<ExpCMD>,
    path: PathBuf,
    _phantom: PhantomData<RT>
}

impl<RT: Runtime + Send + Sync + 'static> Database<RT> {
    /// Loads a database from a directory.
    // this can probably be made to load files semi-concurrently but its fine for now
    pub async fn load(path: impl Into<PathBuf>) -> Result<Self> {
        let path: PathBuf = path.into();
        let (exp_tx, rx) = flume::unbounded::<ExpCMD>();
        let inner = RT::spawn_blocking(move || {
            fs::create_dir_all(&path)?;
            
            let partition_map: SlotMap<ParKey, Partition> = SlotMap::with_key(180);
            let buckets: DashMap<u64, Bucket> = DashMap::new();
            let entry_map: DashMap<u128, CacheEntry> = DashMap::new();
            
            for entry in fs::read_dir(&path)? {
                let entry = entry?;
                if !entry.file_type()?.is_dir() { continue }
                let Some(bucket_id) = entry.file_name().into_string().ok().and_then(|n| n.parse::<u64>().ok()) else { continue };
                let bucket_path = entry.path();
                let bucket_ttl = Duration::from_millis(bucket_id);
                let mut active = ActivePartition::new(ParKey::from_id(SlotId::INVALID), 0);

                for partition in fs::read_dir(&bucket_path)? {
                    let entry = partition?;
                    if !entry.file_type()?.is_file() { continue }
                    let Some(insertion) = entry.file_name().into_string().ok().and_then(|n| n.parse::<u64>().ok()) else { continue };
                    
                    let (keys, partition) = Partition::from_file(entry.path())?;
                    let par_key = partition_map.insert(partition, &partition_map.pin());
                    exp_tx.send(ExpCMD::Schedule { time: insertion + bucket_ttl.as_secs(), par_key }).map_err(Error::flume)?;
                    
                    active = ActivePartition::new(par_key, insertion);  // the last file should be the most recent, and thus the last active one.
                    for (key, entry) in keys {
                        let cache_entry = CacheEntry { partition_key: par_key, position: entry, };
                        entry_map.insert(key, cache_entry);
                    }
                }
                
                buckets.insert(bucket_id, Bucket::new_existing(active, bucket_ttl, bucket_path));
            }

            let inner = DbInner {
                partitions: partition_map,
                entries: entry_map,
                buckets,
                exp_tx,
                path,
                _phantom: PhantomData
            };
            
            Ok::<_, Error>(Arc::new(inner))
        }).await??;

        spawn_expiration_task::<RT>(inner.clone(), rx);
        
        Ok(Self { inner })
    }

    /// Creates a new database without care for previous target directory contents.
    pub fn create_new(path: impl Into<PathBuf>) -> Self {
        let (exp_tx, rx) = flume::unbounded::<ExpCMD>();
        let inner = Arc::new(DbInner {
            buckets: DashMap::new(),
            entries: DashMap::new(),
            partitions: SlotMap::with_key(180),
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

        if !self.inner.buckets.contains_key(&cache_id) {
            let path = self.inner.path.join(format!("{}", ttl.as_millis()));
            let bucket = Bucket::new::<RT>(path, now, ttl, &self.inner.partitions, &self.inner.exp_tx).await?;
            // this access is dropped because we want do not want to hold a lock for the mutable reference accross the coming .await.
            self.inner.buckets.entry(cache_id).or_insert(bucket); 
        }
        
        let bucket = self.inner.buckets.get(&cache_id).ok_or(Error::bucket_str("Bucket was removed before it could be accessed."))?;
        let (partition_key, position) = bucket.insert::<RT>(key, value, &self.inner.partitions, &self.inner.exp_tx).await?;
        self.inner.entries.insert(key, CacheEntry { partition_key, position });
        Ok(())
    }

    /// this returns none if the entry doesnt exist or if the file is deleted.
    pub async fn read(&self, key: u128) -> Result<Option<Bytes>> {
        let Some(CacheEntry { partition_key, position }) = self.inner.entries.get(&key).map(|e|  *e) else {
            return Ok(None)
        };
        
        let guard: Guard<'_> = self.inner.partitions.pin();
        let partition: &Partition = self.inner.partitions.get(partition_key, &guard).ok_or(Error::partition_str("No partition found with cached key."))?;
        partition.read::<RT>(position).await
    }
}
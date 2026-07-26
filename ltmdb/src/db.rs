use std::{fs, hash::{BuildHasher, RandomState}, marker::PhantomData, path::{Path, PathBuf}, sync::Arc, time::Duration};

use bytes::Bytes;
use flume::Sender;
use futures_util::{StreamExt, stream::FuturesUnordered};
use papaya::{HashMap, Operation};
use sharded_slab::Slab;

use crate::{Result, bucket::{ActivePartition, Bucket}, error::Error, expiration_queue::{ExpCMD, run_expiration_task}, partition::{Partition, PartitionEntry}, runtime::Runtime, sized_bytes::SizedBytes, unix_secs};

pub(crate) trait ViableHasher: BuildHasher + Default + Send + Sync + 'static {}
impl<T: BuildHasher + Default + Send + Sync + 'static> ViableHasher for T {}

pub(crate) struct Entry {
    pub key: SizedBytes,
    pub value: Bytes,
}

impl Entry {
    pub fn new(key: impl Into<SizedBytes>, value: impl Into<Bytes>) -> Self {
        Self { key: key.into(), value: value.into() }
    }
}

#[derive(Clone, Copy)]
pub(crate) struct CacheEntry {
    partition_key: usize,
    position: PartitionEntry,
}

impl CacheEntry {
    pub fn new(par_key: usize, position: PartitionEntry) -> Self {
        Self {
            partition_key: par_key,
            position
        }
    }
    
    pub fn par_key(self) -> usize {
        self.partition_key
    }
}

/// Lifetime managed key-value store.
/// Async down to file io (handled by input runtime)
/// Expirations are delegated to a background expiration task and batched by a 1 minute window
/// 
/// Data is synced according to the os.
/// Recently written values must not be assumed to persist on crashes.
#[derive(Clone)]
pub struct Database<RT: Runtime, S: BuildHasher + Default + Send + Sync + 'static = RandomState> {
    maps: Arc<Maps<S>>,
    queue_tx: Sender<ExpCMD>,
    path: PathBuf,
    _phantom: PhantomData<RT>
}

pub(crate) struct Maps<S: ViableHasher = RandomState> {
    pub partitions: Slab<Partition>,
    pub entries: HashMap<SizedBytes, CacheEntry, S>,
    pub buckets: HashMap<u64, Bucket, S>,
}

impl<S: ViableHasher> Maps<S> {
    pub(crate) fn new() -> Self {
        Self {
            partitions: Slab::new(),
            entries: HashMap::with_hasher(S::default()),
            buckets: HashMap::with_hasher(S::default()),
        }
    }
}

impl<RT: Runtime, S: BuildHasher + Default + Send + Sync + 'static> Database<RT, S> {
    fn new(maps: Arc<Maps<S>>, queue_tx: Sender<ExpCMD>, path: PathBuf) -> Self {
        Self {
            maps,
            queue_tx,
            path,
            _phantom: PhantomData,
        }
    }
    
    /// Loads a database from a directory.
    /// 
    /// # Errors
    /// Returns an error if any io operations failed or a spawned task returns an error.
    pub async fn load(path: impl AsRef<Path> + Send + Sync + 'static) -> Result<Self> {
        let (queue_tx, rx) = flume::unbounded::<ExpCMD>();

        let path_buf = path.as_ref().to_path_buf();
        let maps: Arc<Maps<S>> = Arc::new(Maps::<S>::new());

        let inner_ref = &maps; // allows us to move the refs into the closures without moving the values themselves.
        let queue_tx_ref = &queue_tx;
        
        let mut bucket_futures = FuturesUnordered::new();
        for entry in RT::spawn_blocking(move || {
            fs::create_dir_all(&path)?;
            fs::read_dir(&path)
        }).await?? {
            let entry = entry?;
            if !entry.file_type()?.is_dir() { continue }

            let Some(bucket_millis) = entry.file_name().into_string().ok().and_then(|n| n.parse::<u64>().ok()) else { continue };
            let bucket_path = entry.path();
            let bucket_ttl = Duration::from_millis(bucket_millis);
            let bucket_id = bucket_ttl.as_secs();
            let mut last_insertion = 0;
            let mut last_par_key = usize::MAX;

            bucket_futures.push(async move {
                let mut partition_futures = FuturesUnordered::new();

                for entry in RT::spawn_blocking(move || fs::read_dir(bucket_path)).await?? {
                    let entry = entry?;
                    if !entry.file_type()?.is_file() { continue }
                    let Some(insert_time) = entry.file_name().into_string().ok().and_then(|n| n.parse::<u64>().ok()) else { continue };

                    partition_futures.push(async move {
                        let partition_res = RT::spawn_blocking(move || Partition::from_file(insert_time, entry.path())).await.flatten();
                        (insert_time, partition_res)
                    });
                }
                
                while let Some((insert_time, partition_res)) = partition_futures.next().await {
                    let (keys, partition) = partition_res?;

                    let par_key = partition.insert_into(&inner_ref.partitions)?;
                    queue_tx_ref.send(ExpCMD::Schedule { time: insert_time + bucket_id, par_key }).map_err(Error::queue)?;

                    if insert_time > last_insertion {
                        last_insertion = insert_time;
                        last_par_key = par_key;
                    }
                    
                    let guard = inner_ref.entries.guard();
                    for (key, position) in keys {
                        let cache_entry = CacheEntry::new(par_key, position);

                        // this bit ensures only the most recent value is kept. Otherwise, it would be based on task scheduling and which one inserted last.
                        inner_ref.entries.compute(key, |existing| {
                            match existing.and_then(|(_, value)| inner_ref.partitions.get(value.partition_key)) {
                                Some(old) if old.insertion_time > insert_time => Operation::Abort(()), // "old" value is newer
                                _ => Operation::Insert(cache_entry)
                            }
                        }, &guard);
                    }
                }

                let active = ActivePartition::new(last_par_key, last_insertion);
                let bucket = Bucket::new_existing(active, bucket_ttl, entry.path());
                inner_ref.buckets.pin().insert(bucket_id, bucket);
                Ok::<_, Error>(())
            });
        }

        while let Some(res) = bucket_futures.next().await {
            res?;
        }

        drop(bucket_futures); // this drops the references to the inner and queue_tx
        RT::spawn(run_expiration_task::<RT, S>(maps.clone(), rx));
        Ok(Self::new(maps, queue_tx, path_buf))
    }

    /// Creates a new database without care for previous target directory contents.
    pub fn create_new(path: impl Into<PathBuf>) -> Self {
        let (queue_tx, rx) = flume::unbounded::<ExpCMD>();
        let inner = Arc::new(Maps::new());

        RT::spawn(run_expiration_task::<RT, S>(inner.clone(), rx));
        Self::new(inner, queue_tx, path.into())
    }

    /// inserts a key value pair into the database with a given ttl.
    /// 
    /// If an entry already exists, the old value will be replaced with the new.
    /// Old values will remain on disk until their original ttl has expired.
    /// 
    /// # Errors
    /// Returns an error if any io operations failed or a spawned task returns an error.
    #[allow(clippy::used_underscore_items)]
    pub async fn insert(&self, key: impl Into<SizedBytes>, value: impl Into<Bytes>, ttl: Duration) -> Result<()> {
        let now = unix_secs();
        let cache_id = ttl.as_secs();

        let entry = Entry::new(key, value);
        let entry_key = entry.key.clone();
        
        let new_bucket = if self.maps.buckets.pin().contains_key(&cache_id) { None } else {
            let path = self.path.join(ttl.as_millis().to_string());
            Some(Bucket::new::<RT>(path, now, ttl, &self.maps.partitions, &self.queue_tx).await?)
        };

        let insert_future = {
            let guard = self.maps.buckets.guard();

            let bucket = match new_bucket {
                Some(bucket) => self.maps.buckets.get_or_insert(cache_id, bucket, &guard),
                #[allow(clippy::missing_panics_doc)] // this panic should never occur unless we add bucket removal.
                None => self.maps.buckets.get(&cache_id, &guard).expect("new_bucket should be Some if buckets doesnt contain cache_id"),
            };
            bucket.insert::<RT, S>(now, cache_id, entry, &self.maps, &self.queue_tx)
        };
        
        let cache_entry = insert_future.await?;
        self.maps.entries.pin().insert(entry_key, cache_entry);
        Ok(())
    }
    
    /// Attempts to get a value from the database given a key.
    /// Returns Ok(None) if the entry isn't in the database.
    /// 
    /// # Errors
    /// Returns an error if any io operations failed or a spawned task returns an error.
    #[allow(clippy::used_underscore_items)]
    pub async fn read(&self, key: impl Into<SizedBytes>) -> Result<Option<Bytes>> {
        let entry_key = key.into();

        let Some(CacheEntry { partition_key, position }) = self.maps.entries.pin().get(&entry_key).copied() else {
            return Ok(None)
        };

        let Some(read_future) = self.maps.partitions.get(partition_key).map(|p| p.read::<RT>(position)) else {
            return Ok(None) // we can treat missing partitions like a cache miss
        };

        let read = read_future.await?;
        Ok(Some(read))
    }
}

/// Asserts at compile-time that the database's read and insert methods are send safe.
fn _assert_send<RT: Runtime, S: ViableHasher>(db: &Database<RT, S>, key: SizedBytes, value: Bytes) {
    fn assert_send<T: Send>(_: T) { }
    assert_send(db.insert(key.clone(), value, Duration::from_secs(20)));
    assert_send(db.read(key));
}
use std::{fs, future::ready, marker::PhantomData, path::{Path, PathBuf}, sync::Arc, time::Duration};

use bytes::Bytes;
use flume::Sender;
use futures_util::{StreamExt, future::Either, stream::FuturesUnordered};
use papaya::{HashMap, Operation};
use sharded_slab::Slab;

use crate::{ErrorKind, Result, bucket::{ActivePartition, Bucket}, error::Error, expiration_queue::{ExpCMD, run_expiration_task}, hasher::RapidHash, partition::{Partition, PartitionEntry}, runtime::{Runtime, SendRuntime}, sized_bytes::SizedBytes, unix_secs};

#[derive(Clone, Copy)]
pub(crate) struct CacheEntry {
    partition_key: usize,
    position: PartitionEntry,
}

impl CacheEntry {
    pub fn par_key(self) -> usize {
        self.partition_key
    }
}

/// Lifetime managed key-value store.
/// Async down to file io (handled by input runtime)
/// Expirations are delegated to a background expiration task and batched by a 1 minute window
/// 
/// Data is not fsynced ever to keep writes as fast as possible. Recently written values must 
/// not be assumed to persist on crashes.
#[derive(Clone)]
pub struct Database<RT: Runtime + Send + Sync + 'static> {
    inner: Arc<DbInner<RT>>
}

pub(crate) struct DbInner<RT: SendRuntime> {
    partitions: Slab<Partition>,
    entries: HashMap<SizedBytes, CacheEntry, RapidHash>,
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
            partitions: Slab::new(), // if max_capacity is too low, it will panic with `capacity overflow` on insert. 
            exp_tx,
            path: path.into(),
            _phantom: PhantomData
        }
    }
}

impl<RT: Runtime + Send + Sync + 'static> Database<RT> {
    /// Loads a database from a directory.
    /// 
    /// # Errors
    /// Returns an error if any io operations failed or a spawned task returns an error.
    pub async fn load(path: impl AsRef<Path> + Send + Sync + 'static) -> Result<Self> {
        let (exp_tx, rx) = flume::unbounded::<ExpCMD>();

        let inner: Arc<DbInner<RT>> = Arc::new(DbInner::new(exp_tx, path.as_ref()));

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

            let inner = inner.clone();
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

                    let par_key = partition.insert_into(&inner.partitions).ok_or(Error::PARTITION_FAILED_INSERTION)?;
                    inner.exp_tx.send(ExpCMD::Schedule { time: insert_time + bucket_id, par_key }).map_err(Error::queue)?;

                    if insert_time > last_insertion {
                        last_insertion = insert_time;
                        last_par_key = par_key;
                    }
                    
                    let guard = inner.entries.guard();
                    for (key, entry) in keys {
                        let cache_entry = CacheEntry { partition_key: par_key, position: entry, };

                        // this bit ensures only the most recent value is kept. Otherwise, it would be based on task scheduling and which one inserted last.
                        inner.entries.compute(key, |existing| {
                            if let Some((_, value)) = existing {
                                return match inner.partitions.get(value.partition_key) {
                                    Some(old) if old.insertion_time > insert_time => Operation::Abort(()), // "old" value is newer
                                    _ => Operation::Insert(cache_entry)
                                }
                            }
                            Operation::Insert(cache_entry)
                        }, &guard);
                    }
                }

                let active = ActivePartition::new(last_par_key, last_insertion);
                let bucket = Bucket::new_existing(active, bucket_ttl, entry.path());
                inner.buckets.pin().insert(bucket_id, bucket);
                Ok::<_, Error>(())
            });
        }

        while let Some(res) = bucket_futures.next().await {
            res?;
        }
        
        RT::spawn(run_expiration_task::<RT>(DbView::new(inner.clone()), rx));
        Ok(Self { inner })
    }

    /// Creates a new database without care for previous target directory contents.
    pub fn create_new(path: impl Into<PathBuf>) -> Self {
        let (exp_tx, rx) = flume::unbounded::<ExpCMD>();
        let inner = Arc::new(DbInner::new(exp_tx, path));

        RT::spawn(run_expiration_task::<RT>(DbView::new(inner.clone()), rx));
        Self { inner }
    }

    /// inserts a key value pair into the database with a given ttl.
    /// 
    /// If an entry already exists, the old value will be replaced with the new.
    /// Old values will remain on disk until their original ttl has expired.
    /// 
    /// # Errors
    /// Returns an error if any io operations failed or a spawned task returns an error.
    pub async fn insert(&self, key: impl Into<SizedBytes>, value: impl Into<Bytes>, ttl: Duration) -> Result<()> {
        let now = unix_secs();
        let cache_id = ttl.as_secs();

        let mut guard = self.inner.buckets.guard();

        #[allow(clippy::single_match_else)] // clippy is silly and the alternative is a let bucket = if let Some()
        let bucket = match self.inner.buckets.get(&cache_id, &guard) {
            Some(bucket) => bucket,
            None => {
                drop(guard); // drop the guard for the upcoming .await
                
                let path = self.inner.path.join(ttl.as_millis().to_string());
                let bucket = Bucket::new::<RT>(path, now, ttl, &self.inner.partitions, &self.inner.exp_tx).await?;
                
                guard = self.inner.buckets.guard();
                self.inner.buckets.get_or_insert(cache_id, bucket, &guard)
            }
        };

        let entry_key = key.into();
        let insert_future = self.insert_into(bucket, cache_id, entry_key.clone(), value.into());
        drop(guard); // drop the guard for the upcoming .await
        
        let (partition_key, position) = insert_future.await?;
        self.inner.entries.pin().insert(entry_key, CacheEntry { partition_key, position });
        Ok(())
    }

    fn insert_into<'a>(&'a self, bucket: &Bucket, bucket_id: u64, entry_key: SizedBytes, entry_value: Bytes) -> impl Future<Output = Result<(usize, PartitionEntry)>> + use<'a, RT> {
        bucket.insert::<RT>(bucket_id, &self.inner.buckets, entry_key, entry_value, &self.inner.partitions, &self.inner.exp_tx)
    }
    
    /// Attempts to get a value from the database given a key.
    /// Returns Ok(None) if the entry isn't in the database.
    /// 
    /// # Errors
    /// Returns an error if any io operations failed or a spawned task returns an error.
    pub async fn read(&self, key: impl Into<SizedBytes>) -> Result<Option<Bytes>> {
        let Some(CacheEntry { partition_key, position }) = self.inner.entries.pin().get(&key.into()).copied() else {
            return Ok(None)
        };

        let partition = self.inner.partitions.get(partition_key).ok_or(Error::PARTITION_NOT_FOUND)?;
        let read_future = partition.read::<RT>(position);
        drop(partition);
        
        match read_future.await {
            Ok(value) => Ok(Some(value)),
            Err(Error { kind: ErrorKind::PartitionNotFound, .. }) => Ok(None),
            Err(e) => Err(e),
        }
    }
}

/// View into the database. 
/// Used so the expiration task doesnt get full access to the database
/// when it only needs to purge partitions.
pub(crate) struct DbView<RT: SendRuntime> {
    inner: Arc<DbInner<RT>>
}

impl<RT: SendRuntime> DbView<RT> {
    pub(crate) fn new(inner: Arc<DbInner<RT>>) -> Self {
        Self { inner }
    }

    pub(crate) fn purge_partition(&self, key: usize) -> impl Future<Output = Result<()>> + use<RT> {
        let Some(partition) = self.inner.partitions.get(key) else {
            return Either::Left(ready(Err(Error::PARTITION_NOT_FOUND))); // either because i want to return errors on the future itself
        };

        self.inner.partitions.remove(key); // sharded_slab has no problem letting us keep a reference while marking it to be deleted.
        Either::Right(partition.purge::<RT>(&self.inner.entries))
    }
}
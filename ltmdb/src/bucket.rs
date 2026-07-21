use std::{fs, path::PathBuf, sync::atomic::{AtomicBool, Ordering}, time::Duration};

use bytes::Bytes;
use flume::Sender;
use futures_util::future::{Either, ok};
use papaya::HashMap;
use portable_atomic::AtomicU128;
use sharded_slab::Slab;

use crate::{Result, db::CacheEntry, defer::{Deferred, defer}, error::Error, expiration_queue::ExpCMD, hasher::RapidHash, partition::{Partition, PendingPartition}, runtime::Runtime, sized_bytes::SizedBytes, unix_secs};

const BUCKET_WINDOW: Duration = Duration::from_mins(1);

/// State indicating the rotation guard is free to acquire.
const AVAILABLE: bool = false;
/// State indicating the bucket is currently being rotated.
const ROTATATING: bool = true;

#[derive(Debug)]
pub(crate) struct Bucket {
    live_partition: ActivePartition,
    rotation_guard: AtomicBool,
    ttl: Duration,
    path: PathBuf,
}

impl Bucket {
    pub fn new_existing(partition: ActivePartition, ttl: Duration, path: PathBuf) -> Self {
        Self {
            live_partition: partition,
            rotation_guard: AtomicBool::new(AVAILABLE),
            ttl,
            path,
        }
    }

    /// returns a future which will insert an entry into this bucket. 
    /// 
    /// used so that you can drop the [`Bucket`] before the future is polled. this will immedietly 
    /// attempt to acquire the rotation guard, regardless of whether or not the future is polled.
    /// 
    /// requires `bucket_id` and `buckets` to be passed in so the future can reacquire the bucket
    /// when the future has internally finished awaiting.
    pub fn insert<'a, RT: Runtime>(
        &self, 
        bucket_id: u64,
        buckets: &'a HashMap<u64, Bucket, RapidHash>, 
        entry_key: SizedBytes, 
        entry_value: Bytes, 
        partition_map: &'a Slab<Partition>,     
        exp_tx: &'a Sender<ExpCMD>
    ) -> impl Future<Output = Result<CacheEntry>> + use<'a, RT> {
        let now = unix_secs();
        let rotation_future = match self.needs_rotate(now) {
            Ok(path) => Either::Left(async move { // this needs to be a future so the new partition creation can be awaited.
                // ensures the guard will be released if the future is dropped or function returns early.
                let drop_guard = defer(|| buckets.pin().get(&bucket_id).map(Bucket::rel_rotate));

                let partition = PendingPartition::new::<RT>(now, path).await?;
                let new_key = partition.insert_into(partition_map)?;
                
                let bucket_guard = buckets.guard();
                let bucket = buckets.get(&bucket_id, &bucket_guard).ok_or(Error::BUCKET_NOT_FOUND)?;
                exp_tx.send(ExpCMD::Schedule { time: now + bucket.ttl.as_secs(), par_key: new_key }).map_err(Error::queue)?;
                bucket.live_partition.store(new_key, now, Ordering::Relaxed);
                
                bucket.rel_rotate(); // release the rotation guard here so we don't try to re-acquire the `bucket_guard` when we don't need to.
                drop_guard.cancel(); // we have already released the rotation guard, so we need to cancel to prevent it from being run again.

                Ok::<_, Error>(new_key)
            }),
            Err(current_key) => Either::Right(ok(current_key))
        };
        
        async move {
            let par_key = rotation_future.await?;

            let partition = partition_map.get(par_key).ok_or(Error::PARTITION_NOT_FOUND)?;
            let insert_future = partition.insert::<RT>(entry_key, entry_value);
            drop(partition); // drop the partition so we don't prevent its reclamation during the coming .await
            
            let position = insert_future.await?;
            Ok::<_, Error>(CacheEntry::new(par_key, position))
        }
    }

    /// Returns `Ok(pathbuf)` if the bucket needs to be rotated, `Err(ParKey)` otherwise.
    /// Acquires the rotation guard if a rotation is needed.
    pub(crate) fn needs_rotate(&self, now: u64) -> std::result::Result<PathBuf, usize> {
        let (current_key, rotated_at) = self.live_partition.load(Ordering::Relaxed);
        if now < rotated_at + BUCKET_WINDOW.as_secs() {
            return Err(current_key)
        }

        // Attempt to acquire the rotation lock. If this fails, we can safely return a reference to a stale partition.
        // This is fine because we dont promise keys will be deleted exactly on their ttl expiration.
        if !self.acq_rotate() {
            return Err(current_key) 
        }

        // Check for rotation again since another thread may have rotated between our first load and guard acquisition.
        let (current_key, rotated_at) = self.live_partition.load(Ordering::Relaxed);
        if now < rotated_at + BUCKET_WINDOW.as_secs() {
            // Confirmed another thread already rotated, so we can release the guard and return the current key again.
            self.rel_rotate();
            return Err(current_key)
        }

        Ok(self.path.join(now.to_string()))
    }

    pub async fn new<RT: Runtime>(path: PathBuf, now: u64, ttl: Duration, partition_map: &Slab<Partition>, exp_tx: &Sender<ExpCMD>) -> Result<Self> {
        let create_path = path.clone();
        let partition = RT::spawn_blocking(move || {
            fs::create_dir_all(&create_path)?;
            let part_path = create_path.join(now.to_string());
            PendingPartition::new_sync(now, part_path)
        }).await??;
        
        let par_key = partition.insert_into(partition_map)?;
        exp_tx.send(ExpCMD::Schedule { time: now + ttl.as_secs(), par_key }).map_err(Error::queue)?;

        Ok(Self {
            live_partition: ActivePartition::new(par_key, now),
            rotation_guard: AtomicBool::new(AVAILABLE),
            ttl,
            path,
        })
    }

    /// Acquires the rotation guard, returning `true` if the guard was successfully acquired.
    #[inline]
    pub(crate) fn acq_rotate(&self) -> bool {
        self.rotation_guard.compare_exchange(AVAILABLE, ROTATATING, Ordering::AcqRel, Ordering::Acquire).is_ok()
    }

    /// Releases the rotation guard.
    #[inline]
    pub(crate) fn rel_rotate(&self) {
        self.rotation_guard.store(AVAILABLE, Ordering::Release);
    }
}

/// packs a partition key and its insertion time into a single atomic u128.
/// the bits are laid out as
/// ```text
/// | usize (u32 or u63) | u64 (low) |
/// | 128..64            | 63..0     |
/// ```
#[derive(Debug)]
pub(crate) struct ActivePartition {
    value: AtomicU128,
}

impl ActivePartition {
    #[inline]
    pub fn new(key: usize, insertion_time: u64) -> Self {
        Self {
            value: AtomicU128::new(Self::pack(key, insertion_time))
        }
    }

    #[inline]
    #[allow(clippy::cast_possible_truncation)]
    pub fn load(&self, order: Ordering) -> (usize, u64) {
        let data = self.value.load(order);
        ((data >> 64) as usize, data as u64)
    }

    #[inline]
    pub fn store(&self, key: usize, insertion_time: u64, order: Ordering) {
        self.value.store(Self::pack(key, insertion_time), order);
    }

    #[inline]
    fn pack(key: usize, insertion_time: u64) -> u128 {
        (key as u128) << 64 | u128::from(insertion_time)
    }
}
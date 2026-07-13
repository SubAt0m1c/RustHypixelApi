use std::{fs, path::PathBuf, sync::atomic::{AtomicBool, Ordering}, time::Duration};

use bytes::Bytes;
use concurrent_slotmap::SlotMap;
use flume::Sender;
use futures_util::future::{Either, ready};
use papaya::HashMap;
use portable_atomic::AtomicU128;

use crate::{Result, db::ParKey, defer::{Deferred, defer}, error::Error, expiration_queue::ExpCMD, hasher::RapidHash, partition::{Partition, PartitionEntry, PartitionMap}, runtime::SendRuntime, sized_bytes::SizedBytes, unix_secs};

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
    pub fn insert<'a, RT: SendRuntime>(&self, bucket_id: u64, buckets: &'a HashMap<u64, Bucket, RapidHash>, entry_key: SizedBytes, entry_value: Bytes, partition_map: &'a PartitionMap, exp_tx: &'a Sender<ExpCMD>) -> impl Future<Output = Result<(ParKey, PartitionEntry)>> + use<'a, RT> {
        let rotation_future = match self.needs_rotate::<RT>() {
            Ok(path) => Either::Left(async move {
                // ensures the guard will be released if the future is dropped or function returns early.
                let drop_guard = defer(|| buckets.pin().get(&bucket_id).map_or((), Bucket::rel_rotate));
                
                let now = unix_secs();
                let new_partition = Partition::new::<RT>(path).await?;
                let new_key = partition_map.insert(new_partition, &partition_map.pin());
        
                let bucket_guard = buckets.guard();
                let bucket = buckets.get(&bucket_id, &bucket_guard).ok_or(Error::BUCKET_NOT_FOUND)?;
                exp_tx.send(ExpCMD::Schedule { time: now + bucket.ttl.as_secs(), par_key: new_key }).map_err(Error::queue)?;
                bucket.live_partition.store(new_key, now, Ordering::Relaxed);
        
                bucket.rel_rotate(); // release the rotation guard here so we don't try to re-acquire the `bucket_guard` when we don't need to.
                drop_guard.cancel(); // we have already released the rotation guard, so we need to cancel to prevent it from being run again.

                Ok::<_, Error>(new_key)
            }),
            Err(current_key) => Either::Right(ready(Ok(current_key)))
        };
        
        async move {
            let par_key = rotation_future.await?;
            let partition = partition_map.get_ref(par_key).insert::<RT>(entry_key, entry_value).await?;
            Ok::<(ParKey, PartitionEntry), Error>((par_key, partition))
        }
    }

    /// Returns `Ok(pathbuf)` if the bucket needs to be rotated, `Err(ParKey)` otherwise.
    /// Acquires the rotation guard if a rotation is needed.
    fn needs_rotate<RT: SendRuntime>(&self) -> std::result::Result<PathBuf, ParKey> {
        let now = unix_secs();
        let (current_key, rotated_at) = self.live_partition.load(Ordering::Relaxed);
        if now < rotated_at + BUCKET_WINDOW.as_secs() || !self.acq_rotate() {
            return Err(current_key)
        }

        // Check for rotation again since another thread may have rotated between our first load and guard acquisition.
        let (current_key, rotated_at) = self.live_partition.load(Ordering::Relaxed);
        if now < rotated_at + BUCKET_WINDOW.as_secs() {
            self.rel_rotate(); // release the guard since we don't actually need to do a rotate anymore.
            return Err(current_key)
        }

        Ok(self.path.join(now.to_string()))
    }

    pub async fn new<RT: SendRuntime>(path: PathBuf, now: u64, ttl: Duration, partition_map: &SlotMap<ParKey, Partition>, exp_tx: &Sender<ExpCMD>) -> Result<Self> {
        let create_path = path.clone();
        let new_partition = RT::spawn_blocking(move || {
            fs::create_dir_all(&create_path)?;
            let part_path = create_path.join(now.to_string());
            Partition::new_sync(part_path)
        }).await??;
        let par_key = partition_map.insert(new_partition, &partition_map.pin());
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
/// | u32 (high) | u32    | u64 (low) |
/// | 127..96    | 95..64 | 63..0     |
/// ```
#[derive(Debug)]
pub(crate) struct ActivePartition {
    value: AtomicU128,
}

impl ActivePartition {
    #[inline]
    pub fn new(key: ParKey, insertion_time: u64) -> Self {
        Self {
            value: AtomicU128::new(Self::pack(key, insertion_time))
        }
    }

    #[inline]
    pub fn load(&self, order: Ordering) -> (ParKey, u64) {
        let data = self.value.load(order);
        (ParKey::new((data >> 96) as u32, (data >> 64) as u32), data as u64)
    }

    #[inline]
    pub fn store(&self, key: ParKey, insertion_time: u64, order: Ordering) {
        self.value.store(Self::pack(key, insertion_time), order);
    }

    #[inline]
    fn pack(key: ParKey, insertion_time: u64) -> u128 {
        let (index, generation) = key.data();
        u128::from(index) << 96 | u128::from(generation) << 64 | u128::from(insertion_time)
    }
}
use std::{fs, path::PathBuf, sync::{Arc, atomic::{AtomicBool, Ordering}}, time::Duration};

use bytes::Bytes;
use concurrent_slotmap::SlotMap;
use flume::Sender;
use papaya::{HashMap, LocalGuard};
use portable_atomic::AtomicU128;

use crate::{Result, db::ParKey, error::Error, expiration_queue::ExpCMD, hasher::RapidHash, partition::{Partition, PartitionEntry, PartitionMap, PartitionRef}, runtime::SendRuntime, sized_bytes::SizedBytes, unix_secs};

const BUCKET_WINDOW: Duration = Duration::from_mins(1);

/// A reference to an entry in the bucket map.
/// 
/// Contains a key to an entry the bucket map, and a reference to the bucket map.
/// This provides an easy api to reference an entry without holding locks across
/// awaits.
/// 
/// It may be possible to move the guard into the reference, enabling cross-function
/// holding of guards without holding across awaits, but that hasn't been needed yet.
pub(crate) struct BucketRef<'a> {
    key: u64,
    buckets: &'a HashMap<u64, Bucket, RapidHash>,
}

impl<'a> BucketRef<'a> {
    pub fn new(key: u64, buckets: &'a HashMap<u64, Bucket, RapidHash>) -> Self {
        Self { key, buckets, }
    }

    pub async fn insert<RT: SendRuntime>(&self, guard: LocalGuard<'_>, entry_key: SizedBytes, entry_value: Bytes, partition_map: &PartitionMap, exp_tx: &Sender<ExpCMD>) -> Result<(ParKey, PartitionEntry)> {
        let partition = self.get_live_partition::<RT>(guard, unix_secs(), partition_map, exp_tx).await?;
        let partition_entry = partition.insert::<RT>(entry_key, entry_value).await?;
        
        Ok((partition.key(), partition_entry))
    }

    async fn get_live_partition<'b, RT: SendRuntime>(&self, bucket_guard: LocalGuard<'_>, now: u64, partition_map: &'b PartitionMap, exp_tx: &Sender<ExpCMD>) -> Result<PartitionRef<'b>> {
        let bucket = self.buckets.get(&self.key, &bucket_guard).ok_or(Error::BUCKET_NOT_FOUND)?;
        let (current_key, rotated_at) = bucket.live_partition.load();

        // If the rotation lock cannot be acquired, we return the current partition.
        // We don't need to immedietly worry about writing to an old partition, we actively promise that removals may be off in timing.
        if now < rotated_at + BUCKET_WINDOW.as_secs() || !bucket.acq_rotate() {
            return Ok(partition_map.get_ref(current_key))
        }
        
        // this will reset the guard if the function returns early or the future is dropped/cancelled.
        let _drop_guard = reset_on_drop(bucket.rotation_guard.clone()); // we need a cloned arc here so we dont hold the bucket guard while awaiting a new partition being made.
        let path = bucket.path.join(now.to_string());
        drop(bucket_guard);

        let new_partition = Partition::new::<RT>(path).await?;
        let new_key = partition_map.insert(new_partition, &partition_map.pin());

        let bucket_guard = self.buckets.guard();
        let bucket = self.buckets.get(&self.key, &bucket_guard).ok_or(Error::BUCKET_NOT_FOUND)?;
        exp_tx.send(ExpCMD::Schedule { time: now + bucket.ttl.as_secs(), par_key: new_key }).map_err(Error::queue)?;
        bucket.live_partition.store(new_key, now);

        Ok(partition_map.get_ref(new_key))
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
    pub fn load(&self) -> (ParKey, u64) {
        let data = self.value.load(Ordering::Relaxed);
        (ParKey::new((data >> 96) as u32, (data >> 64) as u32), data as u64)
    }

    #[inline]
    pub fn store(&self, key: ParKey, insertion_time: u64) {
        self.value.store(Self::pack(key, insertion_time), Ordering::Relaxed);
    }

    #[inline]
    fn pack(key: ParKey, insertion_time: u64) -> u128 {
        let (index, generation) = key.data();
        u128::from(index) << 96 | u128::from(generation) << 64 | u128::from(insertion_time)
    }
}

#[derive(Debug)]
pub(crate) struct Bucket {
    live_partition: ActivePartition,
    rotation_guard: Arc<AtomicBool>,
    ttl: Duration,
    path: PathBuf,
}

impl Bucket {
    pub fn new_existing(partition: ActivePartition, ttl: Duration, path: PathBuf) -> Self {
        Self {
            live_partition: partition,
            rotation_guard: Arc::new(AtomicBool::new(false)),
            ttl,
            path,
        }
    }
    
    pub async fn new<RT: SendRuntime>(path: PathBuf, now: u64, ttl: Duration, partition_map: &SlotMap<ParKey, Partition>, exp_tx: &Sender<ExpCMD>) -> Result<Self> {
        let create_path = path.clone();
        let new_partition = RT::spawn_blocking(move || {
            fs::create_dir_all(&create_path)?;
            let part_path = create_path.join(format!("{now}"));
            Partition::new_sync(part_path)
        }).await??;
        let par_key = partition_map.insert(new_partition, &partition_map.pin());
        exp_tx.send(ExpCMD::Schedule { time: now + ttl.as_secs(), par_key }).map_err(Error::queue)?;
        
        Ok(Self {
            live_partition: ActivePartition::new(par_key, now),
            rotation_guard: Arc::new(AtomicBool::new(false)),
            ttl,
            path,
        })
    }

    /// Acquires the rotation guard, returning `true` if the guard was successfully acquired.
    /// 
    /// rotation guard must be set to `false` when the rotation is complete.
    fn acq_rotate(&self) -> bool {
        self.rotation_guard.compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire).is_ok()
    }
}

fn reset_on_drop(guard: Arc<AtomicBool>) -> ResetOnDrop {
    ResetOnDrop(guard)
}

struct ResetOnDrop(Arc<AtomicBool>);

impl Drop for ResetOnDrop {
    fn drop(&mut self) {
        self.0.store(false, Ordering::Release);
    }
}
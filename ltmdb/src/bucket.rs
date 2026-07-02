use std::{fs, path::PathBuf, sync::atomic::Ordering, time::Duration};

use bytes::Bytes;
use concurrent_slotmap::SlotMap;
use flume::Sender;
use papaya::{HashMap, LocalGuard};
use portable_atomic::AtomicU128;

use crate::{Result, db::ParKey, error::Error, expiration_queue::ExpCMD, partition::{Partition, PartitionEntry, PartitionMap, PartitionRef}, runtime::SendRuntime, unix_secs};

const BUCKET_WINDOW: Duration = Duration::from_mins(1);

pub(crate) struct BucketRef<'a> {
    key: u64,
    buckets: &'a HashMap<u64, Bucket>,
}

impl<'a> BucketRef<'a> {
    pub fn new(key: u64, buckets: &'a HashMap<u64, Bucket>) -> Self {
        Self { key, buckets, }
    }

    pub async fn insert<RT: SendRuntime>(&self, guard: LocalGuard<'_>, entry_key: u128, entry_value: Bytes, partition_map: &PartitionMap, exp_tx: &Sender<ExpCMD>) -> Result<(ParKey, PartitionEntry)> {
        let partition = self.get_live_partition::<RT>(guard, unix_secs(), partition_map, exp_tx).await?;
        let partition_entry = partition.insert::<RT>(entry_key, entry_value).await?;
        
        Ok((partition.key(), partition_entry))
    }

    async fn get_live_partition<'b, RT: SendRuntime>(&self, guard: LocalGuard<'_>, now: u64, partition_map: &'b PartitionMap, exp_tx: &Sender<ExpCMD>) -> Result<PartitionRef<'b>> {
        let bucket = self.buckets.get(&self.key, &guard).ok_or(Error::BUCKET_NOT_FOUND)?;
        let (current_key, rotated_at) = bucket.live_partition.load();

        if now < rotated_at + BUCKET_WINDOW.as_secs() {
            return Ok(partition_map.get_ref(current_key))
        }

        let path = bucket.path.join(now.to_string());
        drop(guard);

        let new_partition = Partition::new::<RT>(path).await?;
        let new_key = partition_map.insert(new_partition, &partition_map.pin());

        {
            let guard = self.buckets.guard();
            let bucket = self.buckets.get(&self.key, &guard).ok_or(Error::BUCKET_NOT_FOUND)?;
            exp_tx.send(ExpCMD::Schedule { time: now + bucket.ttl.as_secs(), par_key: new_key }).map_err(Error::queue)?;
            bucket.live_partition.store(new_key, now);
        }

        Ok(partition_map.get_ref(new_key))
    }
}

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
        (index as u128) << 96 | (generation as u128) << 64 | (insertion_time as u128)
    }
}

pub(crate) struct Bucket {
    live_partition: ActivePartition,
    // live_window: Duration,
    ttl: Duration,
    path: PathBuf,
}

impl Bucket {
    pub fn new_existing(partition: ActivePartition, ttl: Duration, path: PathBuf) -> Self {
        Self {
            live_partition: partition,
            // live_window,
            ttl,
            path,
        }
    }
    
    pub async fn new<RT: SendRuntime>(path: PathBuf, now: u64, ttl: Duration, partition_map: &SlotMap<ParKey, Partition>, exp_tx: &Sender<ExpCMD>) -> Result<Self> {
        let create_path = path.clone();
        RT::spawn_blocking(move || fs::create_dir_all(create_path)).await??;
        let part_path = path.join(format!("{}", now));
        let new_partition = Partition::new::<RT>(part_path).await?;
        let par_key = partition_map.insert(new_partition, &partition_map.pin());
        exp_tx.send(ExpCMD::Schedule { time: now + ttl.as_secs(), par_key }).map_err(Error::queue)?;
        
        Ok(Self {
            live_partition: ActivePartition::new(par_key, now),
            ttl,
            path,
        })
    }
}

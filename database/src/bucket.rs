use std::{fs, path::PathBuf, sync::atomic::Ordering, time::Duration};

use atomic::Atomic;
use bytemuck::{Pod, Zeroable};
use bytes::Bytes;
use concurrent_slotmap::{hyaline::Guard, SlotMap};
use flume::Sender;

use crate::{Result, cache::ParKey, error::Error, expiration_queue::ExpCMD, partition::{Partition, PartitionEntry}, runtime::SendRuntime, unix_secs};

const BUCKET_WINDOW: Duration = Duration::from_mins(1);

#[derive(Clone, Copy)]
pub struct ActivePartition {
    key: u64,
    insertion_time: u64
}

impl ActivePartition {
    #[inline]
    pub fn new(key: ParKey, insertion_time: u64) -> Self {
        Self { key: key.encode(), insertion_time }
    }

    #[inline]
    pub fn key(&self) -> ParKey {
        ParKey::decode(self.key)
    }
    
    #[inline]
    pub fn elapsed(&self, now: u64, window: &Duration) -> bool {
        now >= self.insertion_time + window.as_secs()
    }
}

// SAFETY: Literally two u64s.
// i mean technically the encoding/decoding of the slotmap makes 0 not actually "valid" but thats not my problem i think? idk.
unsafe impl Zeroable for ActivePartition {}
unsafe impl Pod for ActivePartition {}

pub struct Bucket {
    live_partition: Atomic<ActivePartition>,
    // live_window: Duration,
    ttl: Duration,
    path: PathBuf,
}

impl Bucket {
    pub fn new_existing(partition: ActivePartition, ttl: Duration, path: PathBuf) -> Self {
        Self {
            live_partition: Atomic::new(partition),
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
        exp_tx.send(ExpCMD::Schedule { time: now + ttl.as_secs(), par_key }).map_err(Error::flume)?;
        
        Ok(Self {
            live_partition: Atomic::new(ActivePartition { key: par_key.encode(), insertion_time: now }),
            // live_window: Duration::from_secs(60),
            ttl,
            path,
        })
    }

    pub async fn insert<RT: SendRuntime>(&self, entry_key: u128, value: Bytes, partition_map: &SlotMap<ParKey, Partition>, exp_tx: &Sender<ExpCMD>) -> Result<(ParKey, PartitionEntry)>{ 
        let now = unix_secs();
        let active = self.live_partition.load(Ordering::Relaxed);
        let guard: Guard<'_> = partition_map.pin();
        
        let (partition, key): (&Partition, ParKey) = if active.elapsed(now, &BUCKET_WINDOW) {
            let key: ParKey = self.rotate::<RT>(now, partition_map, exp_tx).await?;
            let partition = partition_map.get(key, &guard).ok_or(Error::partition_str("No partition with newly rotated partition key."))?;
            (partition, key)
        } else {
            let key = active.key();
            let partition: &Partition = partition_map.get(key, &guard).ok_or(Error::partition_str("No partition found with live_partition key."))?;
            (partition, key)
        };
        
        Ok((key, partition.insert::<RT>(entry_key, value).await?))
    }

    async fn rotate<RT: SendRuntime>(&self, now: u64, partition_map: &SlotMap<ParKey, Partition>, exp_tx: &Sender<ExpCMD>) -> Result<ParKey> {
        let part_path = self.path.join(format!("{}", now));
        let new_partition = Partition::new::<RT>(part_path).await?;
        let par_key = partition_map.insert(new_partition, &partition_map.pin());
        exp_tx.send(ExpCMD::Schedule { time: now + self.ttl.as_secs(), par_key }).map_err(Error::flume)?;
        self.live_partition.store(ActivePartition::new(par_key, now), Ordering::Relaxed);
        Ok(par_key)
    }
}

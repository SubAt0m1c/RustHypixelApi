use std::{fs::File, io::{self, Read, Seek}, ops::Deref, path::PathBuf};

use bytes::{Buf, Bytes, BytesMut};
use concurrent_slotmap::SlotMap;
use crossbeam_queue::SegQueue;
use papaya::HashMap;

use crate::{Error, ErrorKind, Result, db::{CacheEntry, ParKey}, file_handle::FileHandle, runtime::SendRuntime, sized_bytes::SizedBytes};

const BUFFER_SIZE: usize = 8 * 1024 * 1024; // 8mb
const ENTRYMETADATALENGTH: usize = size_of::<u128>() + size_of::<u64>() + size_of::<u64>();

pub(crate) struct PartitionMap {
    map: SlotMap<ParKey, Partition>
}

impl PartitionMap {
    pub fn new(max_capacity: u32) -> Self {
        Self { map: SlotMap::with_key(max_capacity) }
    }
    
    pub fn get_ref(&self, key: ParKey) -> PartitionRef<'_> {
        PartitionRef::new(key, &self)
    }
}

impl Deref for PartitionMap {
    type Target = SlotMap<ParKey, Partition>;

    fn deref(&self) -> &Self::Target {
        &self.map
    }
}

#[derive(Clone, Copy)]
pub(crate) struct PartitionEntry {
    position: u64,
    value_len: usize
}

pub(crate) struct PartitionRef<'a> {
    key: ParKey,
    partitions: &'a PartitionMap
}

impl<'a> PartitionRef<'a> {
    pub fn new(key: ParKey, partitions: &'a PartitionMap) -> Self {
        Self { key, partitions }
    }

    pub fn key(&self) -> ParKey {
        self.key
    }

    pub async fn insert<RT: SendRuntime>(&self, entry_key: u128, entry_value: Bytes) -> Result<PartitionEntry> {
        let key_len = size_of::<u128>() as u64;
        
        let key_len_buf = SizedBytes::from(key_len.to_be_bytes());
        let key_buf = SizedBytes::from(entry_key.to_be_bytes());
        
        let value_len = entry_value.len() as u64;
        let value_len_buf = SizedBytes::from(value_len.to_be_bytes());

        // this is chained to avoid allocating and doing nonsense to another buffer with the input value.
        let chain = Buf::chain(key_len_buf, key_buf)
            .chain(value_len_buf)
            .chain(entry_value);

        let write_location = self.append_from::<RT, _>(chain).await?;

        self.insert_key(entry_key);
        
        Ok(PartitionEntry {
            position: write_location + ENTRYMETADATALENGTH as u64,
            value_len: value_len as usize
        })
    }

    #[inline]
    pub async fn read<RT: SendRuntime>(&self, position: PartitionEntry) -> Result<Bytes> {
        let fut = {
            let guard = self.partitions.pin();
            let partition = self.partitions.get(self.key, &guard).ok_or(Error::PARTITION_NOT_FOUND)?;
            partition.file.read_to::<RT>(position.position, BytesMut::zeroed(position.value_len))
        };
        fut.await.map(|b| b.freeze())
    }
    
    /// This removes all keys from this partition that are shared by the entries dashmap
    /// After removing these keys, it returns a future to a pending file deletion.
    /// This will delete keys immedietly without being polled and on poll will delete the file.
    pub async fn purge<RT: SendRuntime>(&self, entries: &HashMap<u128, CacheEntry>) -> Result<()> {
        let fut = {
            let guard = self.partitions.pin();
            let partition = self.partitions.get(self.key, &guard).ok_or(Error::PARTITION_NOT_FOUND)?;
            
            let guard = entries.guard();
            while let Some(key) = partition.keys.pop() {
                entries.remove(&key, &guard);
            }; 
            partition.file.delete::<RT>()
        };
        fut.await
    }

    async fn append_from<RT: SendRuntime, B: Buf + Send + Sync + 'static>(&self, buf: B) -> Result<u64> {
        let fut = {
            let guard = self.partitions.pin();
            let partition = self.partitions.get(self.key, &guard).ok_or(Error::simple(ErrorKind::PartitionError, "Partition not found."))?;
            partition.file.append_from::<RT, _>(buf)
        };
        fut.await
    }

    fn insert_key(&self, key: u128) {
        self.partitions.get(self.key, &self.partitions.pin()).inspect(|b| b.keys.push(key));
    }
}

pub(crate) struct Partition {
    pub file: FileHandle,
    pub keys: SegQueue<u128>
}

impl Partition {
    pub async fn new<RT: SendRuntime>(path: PathBuf) -> Result<Self> {
        let inner = Partition {
            file: FileHandle::new::<RT>(path).await?,
            keys: SegQueue::new(),
        };
        Ok(inner)
    }

    /// creates a partition file by reading an existing file. Returns a vec of key/entry pairs it contains.
    pub fn from_file(path: PathBuf) -> Result<(Vec<(u128, PartitionEntry)>, Self)> {
        let mut file = File::options().read(true).open(&path)?;
        let mut buffer = BytesMut::with_capacity(8 * 1024 * 1024);
        let mut entries: Vec<(u128, PartitionEntry)> = Vec::new();
        let keys: SegQueue<u128> = SegQueue::new();
        let mut position: usize = 0;
        
        loop {
            let read = fill(&mut file, &mut buffer)?; 
            if read == 0 { break; } // EOF

            if buffer.len() <= ENTRYMETADATALENGTH { continue } // attempt to refill the buffer if it came up short
            position += ENTRYMETADATALENGTH;
            
            let _key_len = buffer.get_u64();
            let key = buffer.get_u128();
            keys.push(key);
            let value_len = buffer.get_u64() as usize;

            if buffer.remaining() >= value_len {
                buffer.advance(value_len);
            } else {
                let read = buffer.remaining();
                buffer.clear(); // clear the buffer so the start of the buffer is free for the next entry's metadata.
                file.seek_relative((value_len - read) as i64)?;
            }
            
            entries.push((key, PartitionEntry { position: position as u64, value_len}));
            position += value_len
        }

        let inner = Self {
            file: FileHandle::new_sync(path)?,
            keys,
        };
        
        Ok((entries, inner))
    }
}

fn fill<R: Read>(reader: &mut R, buf: &mut BytesMut) -> io::Result<usize> {
    let _ = buf.try_reclaim(BUFFER_SIZE); // we dont really care if it reclaimed every byte, it just needs to try to reclaim as much as possible.
    
    let spare = buf.spare_capacity_mut();

    // SAFETY: We don't read from this and we only set its length for as much as was read.
    let dst = unsafe {
        std::slice::from_raw_parts_mut(
            spare.as_mut_ptr() as *mut u8,
            spare.len(),
        )
    };

    let n = reader.read(dst)?;

    // SAFETY: Read::read returns the number of written bytes.
    unsafe {
        buf.set_len(buf.len() + n);
    }

    Ok(n)
}
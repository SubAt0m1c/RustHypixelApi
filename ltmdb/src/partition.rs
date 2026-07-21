use std::{io::{self, Read, Seek}, path::PathBuf};

use bytes::{Buf, Bytes, BytesMut};
use crossbeam_queue::SegQueue;
use futures_util::TryFutureExt;
use papaya::HashMap;
use sharded_slab::Slab;

use crate::{Error, Result, db::CacheEntry, file_handle::{FileHandle, open_file}, hasher::RapidHash, runtime::Runtime, sized_bytes::SizedBytes};

const KEY_LEN_SIZE: usize = size_of::<u64>();
const VALUE_LEN_SIZE: usize = size_of::<u64>();

#[derive(Clone, Copy)]
pub(crate) struct PartitionEntry {
    position: u64,
    value_len: usize
}

/// A partition that doesn't hold its own key yet.
/// 
/// Used to prevent `FileHandle` creation while holding a reference to a partition slab entry
pub(crate) struct PendingPartition {
    insertion_time: u64,
    file: FileHandle,
    keys: SegQueue<SizedBytes>
}

impl PendingPartition {
    pub async fn new<RT: Runtime>(now: u64, path: PathBuf) -> Result<Self> {
        Ok(Self {
            insertion_time: now,
            file: FileHandle::new::<RT>(path).await?,
            keys: SegQueue::new(),
        })
    }
    
    pub fn new_sync(now: u64, path: PathBuf) -> Result<Self> {
        Ok(Self {
            insertion_time: now,
            file: FileHandle::new_sync(path)?,
            keys: SegQueue::new(),
        })
    }

    /// Inserts this pending partition into the given slab of partitions.
    /// 
    /// Returns the key of the inserted partition, or `None` if the slab is full.
    pub fn insert_into(self, partitions: &Slab<Partition>) -> Result<usize> {
        let vacent = partitions.vacant_entry().ok_or(Error::PARTITION_FAILED_INSERTION)?;
        let key = vacent.key();
        vacent.insert(self.construct(key));
        Ok(key)
    }
    
    fn construct(self, key: usize) -> Partition {
        Partition {
            insertion_time: self.insertion_time,
            key,
            file: self.file,
            keys: self.keys,
        }
    }
}

pub(crate) struct Partition {
    pub insertion_time: u64,
    pub key: usize,
    pub file: FileHandle,
    pub keys: SegQueue<SizedBytes>
}

impl Partition {
    /// Inserts a key-value pair into this partition, returning a future that resolves to the entry position.
    /// Keys are inserted into the partition's queue without being polled.
    #[allow(clippy::cast_possible_truncation)]
    #[must_use = "This future has side effects before being polled!"]
    pub fn insert<RT: Runtime>(&self, entry_key: SizedBytes, entry_value: Bytes) -> impl Future<Output = Result<PartitionEntry>> + use<RT> {
        let key_len = entry_key.len() as u64;
        let value_len = entry_value.len() as u64;
        
        let buf;
        #[cfg(unix)]
        {
            let key_len_buf = SizedBytes::from(key_len.to_be_bytes());
            let value_len_buf = SizedBytes::from(value_len.to_be_bytes());
    
            // Chaining here avoids the allocation/move of a large key and/or value required to put them in one buffer, but this only helps when we have pwritev support.
            buf = Buf::chain(key_len_buf, entry_key.clone())
                .chain(value_len_buf)
                .chain(entry_value);
        }
        
        #[cfg(not(unix))]
        {
            use bytes::BufMut;

            let mut buffer = BytesMut::with_capacity(KEY_LEN_SIZE + entry_key.len() + VALUE_LEN_SIZE + entry_value.len());
            buffer.put_u64(key_len);
            buffer.put_slice(&entry_key);
            buffer.put_u64(value_len);
            buffer.put(entry_value);

            // writing a whole vector at once reduces syscalls; a chain would require each chunk to be written individually.
            buf = buffer.freeze();
        }
        
        self.keys.push(entry_key);
        self.append_from::<RT, _>(buf).map_ok(move |write_location| {
            PartitionEntry {
                position: write_location + KEY_LEN_SIZE as u64 + key_len + VALUE_LEN_SIZE as u64,
                value_len: value_len as usize
            }
        })
    }

    /// This removes all keys from this partition that are shared by the entries dashmap
    /// After removing these keys, it returns a future to a pending file deletion.
    /// This will delete keys immedietly without being polled and on poll will delete the file.
    #[must_use = "This future has side effects before being polled!"]
    pub fn purge<RT: Runtime>(&self, entries: &HashMap<SizedBytes, CacheEntry, RapidHash>) -> impl Future<Output = Result<()>> + use<RT> {
        let guard = entries.guard();
        while let Some(key) = self.keys.pop() {
            let _ = entries.remove_if(&key, |_, v| v.par_key() == self.key, &guard);
        }
        self.file.delete::<RT>()
    }

    pub fn read<RT: Runtime>(&self, position: PartitionEntry) -> impl Future<Output = Result<Bytes>> + use<RT> {
        self.file.read_to::<RT>(position.position, BytesMut::zeroed(position.value_len)).map_ok(BytesMut::freeze)
    }

    pub fn append_from<RT: Runtime, B: Buf + Send + Sync + 'static>(&self, buf: B) -> impl Future<Output = Result<u64>> + use<RT, B> {
        self.file.append_from::<RT, _>(buf)
    }
    
    /// creates a partition file by reading an existing file. Returns a partition pending key insertion.
    #[allow(clippy::cast_possible_truncation, clippy::cast_possible_wrap)]
    pub fn from_file(now: u64, path: PathBuf) -> Result<(Vec<(SizedBytes, PartitionEntry)>, PendingPartition)> {
        const BUFFER_SIZE: usize = 8 * 1024 * 1024; // 8mb
        
        let mut file = open_file(&path)?;
        let mut buffer = BytesMut::with_capacity(BUFFER_SIZE);
        let mut entries: Vec<(SizedBytes, PartitionEntry)> = Vec::new();
        let keys: SegQueue<SizedBytes> = SegQueue::new();
        let mut position: usize = 0;

        loop {
            let read = fill(&mut file, &mut buffer)?; 
            if read == 0 { break; } // EOF

            // Attempt to refill the buffer if it isnt long enough to read all needed metadata
            if buffer.len() < KEY_LEN_SIZE { continue }
            let key_len = u64::from_be_bytes(buffer.chunk()[..KEY_LEN_SIZE].try_into().expect("Should have verified buffer is long enough")) as usize; 
            let entry_metadata_len = KEY_LEN_SIZE + key_len + VALUE_LEN_SIZE;
            
            // Reserve space if the entry metadata is longer than `BUFFER_SIZE`
            if buffer.capacity() < entry_metadata_len { buffer.reserve(entry_metadata_len - buffer.len()); }
            if buffer.len() < entry_metadata_len { continue }

            position += entry_metadata_len;
            
            buffer.advance(KEY_LEN_SIZE); // we previously already got key length and dont need to get it again, just advance as if we did.
            let key = SizedBytes::from(&buffer.chunk()[..key_len]);
            buffer.advance(key_len);
            keys.push(key.clone());
            
            let value_len = buffer.get_u64() as usize;
            if buffer.remaining() >= value_len {
                buffer.advance(value_len);
            } else {
                let read = buffer.remaining();
                buffer.clear(); // clear the buffer so the start of the buffer is free for the next entry's metadata.
                file.seek_relative((value_len - read) as i64)?;
            }
            
            entries.push((key, PartitionEntry { position: position as u64, value_len}));
            position += value_len;
        }

        let inner = PendingPartition {
            insertion_time: now,
            file: FileHandle::from_file(file, path)?,
            keys,
        };
        
        Ok((entries, inner))
    }
}

fn fill<R: Read>(reader: &mut R, buf: &mut BytesMut) -> io::Result<usize> {
    /// This is how much space we check the buffer has before attempting to reclaim.
    /// This may be able to be optimized out, only reclaiming when we dont have the
    /// space to store the next entrys metadata exactly, but since this is only on
    /// db load, im not too worried about possible extra syscalls.
    const RECLAIM_SIZE: usize = 512 * 1024; // 512kb
    let _ = buf.try_reclaim(RECLAIM_SIZE); // we dont really care if it reclaimed every byte, just enough to generally ensure we get the next entry's metadata.
    
    let spare = buf.spare_capacity_mut();

    // SAFETY: We don't read from this and we only set its length for as much as was read.
    let dst = unsafe {
        std::slice::from_raw_parts_mut(
            spare.as_mut_ptr().cast::<u8>(),
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
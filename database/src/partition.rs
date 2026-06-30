use std::{fs::File, io::{self, Read, Seek}, path::PathBuf};

use bytes::{Buf, Bytes, BytesMut};
use dashmap::DashMap;
use parking_lot::Mutex;

use crate::{cache::CacheEntry, file_handle::FileHandle, runtime::SendRuntime, sized_bytes::SizedBytes, Result};

const BUFFER_SIZE: usize = 8 * 1024 * 1024; // 8mb

#[derive(Clone, Copy)]
pub struct PartitionEntry {
    position: u64,
    value_len: usize
}

pub struct Partition {
    file: FileHandle,
    keys: Mutex<Vec<u128>> // mutex because its append only and should be fine under stress. Can be changed to be lock free if needed.
}

impl Partition {
    pub async fn new<RT: SendRuntime>(path: PathBuf) -> Result<Self> {
        let file = FileHandle::new::<RT>(path).await?;
        Ok(Self { file, keys: Mutex::new(Vec::new()) })
    }

    /// creates a partition file by reading an existing file. Returns a vec of key/entry pairs it contains.
    pub fn from_file(path: PathBuf) -> Result<(Vec<(u128, PartitionEntry)>, Self)> {
        const ENTRYMETADATALENGTH: usize = size_of::<u128>() + size_of::<u64>() + size_of::<u64>();
        
        let mut file = File::options().read(true).open(&path)?;
        let mut buffer = BytesMut::with_capacity(8 * 1024 * 1024);
        let mut entries: Vec<(u128, PartitionEntry)> = Vec::new();
        let mut keys: Vec<u128> = Vec::new();
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

            if buffer.remaining() >= value_len{
                buffer.advance(value_len);
            } else {
                let read = buffer.remaining(); // gets how much of the value has already been written to the buffer
                buffer.clear(); // rest of the buffer is an entry, we dont need it.
                file.seek_relative((value_len - read) as i64)?; // skips the rest of the entry
                // the buffer should be refilled starting at the next key.
            }
            
            entries.push((key, PartitionEntry { position: position as u64, value_len}));
            position += value_len
        }

        let partition = Self {
            file: FileHandle::new_sync(path)?,
            keys: Mutex::new(keys)
        };
        
        Ok((entries, partition))
    }

    pub async fn insert<RT: SendRuntime>(&self, key: u128, value: Bytes) -> Result<PartitionEntry> {
        let key_len = size_of::<u128>() as u64;
        let value_len = value.len() as u64;
        
        let key_len_buf = SizedBytes::from(key_len.to_be_bytes());
        let key_buf = SizedBytes::from(key.to_be_bytes());
        let value_len_buf = SizedBytes::from(value_len.to_be_bytes());

        // this is chained to avoid allocating and doing nonsense to another buffer with the input value.
        let chain = Buf::chain(key_len_buf, key_buf)
            .chain(value_len_buf)
            .chain(value);

        let write_location = self.file.append_from::<RT, _>(chain).await?;
        self.keys.lock().push(key);
        
        let position = write_location + size_of::<u64>() as u64 + key_len + size_of::<u64>() as u64;
        
        Ok(PartitionEntry {
            position: position,
            value_len: value_len as usize
        })
    }

    #[inline]
    pub async fn read<RT: SendRuntime>(&self, position: PartitionEntry) -> Result<Option<Bytes>> {
        self.file.read_to::<RT>(position.position, BytesMut::zeroed(position.value_len)).await.map(|b| b.map(|b| b.freeze()))
    }
    
    /// This removes all keys from this partition that are shared by the entries dashmap
    /// After removing these keys, it returns a future to a pending file deletion.
    /// This will delete keys immedietly without being polled and on poll will delete the file.
    pub fn purge<RT: SendRuntime>(&self, entries: &DashMap<u128, CacheEntry>) -> impl Future<Output = Result<()>> + use<RT> {
        let keys: Vec<u128> = std::mem::replace(self.keys.lock().as_mut(), Vec::new());
        for key in keys {
            entries.remove(&key);
        }
        self.file.delete::<RT>()   
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
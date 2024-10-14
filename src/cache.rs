use lz4::{EncoderBuilder, Decoder};
use std::io::{Read, Write};
use std::sync::{Arc, Mutex};
use lru::LruCache;
use chrono::{DateTime, Utc};
use serde_json::Value;

///Maximum memory usable before old values are removed (first digit is MBs)
const MAX_MEMORY_USAGE: usize = 256 * 1024 * 1024;
///How long before the cached values are allowed to be fetched again.
const CACHE_EXPIRATION_SECONDS: i64 = 300;
///Maximum entries in the cache. After the limit is reached, the oldest entries will be dropped.
const CACHE_SIZE: usize = 300;

#[derive(Debug)]
struct CacheEntry {
    data: Vec<u8>,
    inserted_at: DateTime<Utc>,
}

#[derive(Debug)]
pub struct Cache {
    map: LruCache<String, CacheEntry>,
    current_memory_usage: usize,
}

impl Cache {
    pub fn new(capacity: usize) -> Self {
        Cache {
            map: LruCache::new(capacity),
            current_memory_usage: 0,
        }
    }

    pub fn create() -> Arc<Mutex<Cache>> {
        Arc::new(Mutex::new(Cache::new(CACHE_SIZE)))
    }

    pub fn insert(&mut self, key: String, json: &Value) {
        let json_str = serde_json::to_string(json).unwrap();

        let compressed_data = compress_data(json_str.as_bytes()).expect("Failed to compress data");

        let entry_size = compressed_data.len();

        while self.current_memory_usage + entry_size > MAX_MEMORY_USAGE {
            self.evict();
        }

        let entry = CacheEntry {
            data: compressed_data,
            inserted_at: Utc::now(),
        };
        self.map.put(key.clone(), entry);
        self.current_memory_usage += entry_size;

        println!("Current memory usage: {}", self.current_memory_usage)
    }

    fn evict(&mut self) {
        if let Some((key, _)) = self.map.pop_lru() {
            if let Some(entry) = self.map.get(&key) {
                self.current_memory_usage -= entry.data.len();
            }
        }
    }

    pub fn get(&mut self, key: &str) -> Option<Value> {
        if let Some(entry) = self.map.get_mut(key) {
            let now = Utc::now();
            if now.signed_duration_since(entry.inserted_at).num_seconds() < CACHE_EXPIRATION_SECONDS {
                let decompressed_data = decompress_data(&entry.data).expect("Failed to decompress data");
                return Some(serde_json::from_slice(&decompressed_data).unwrap());
            } else {
                self.current_memory_usage -= entry.data.len();
                self.map.pop(key);
            }
        }
        None
    }
}

fn compress_data(data: &[u8]) -> Result<Vec<u8>, std::io::Error> {
    let mut encoder = EncoderBuilder::new().build(Vec::new())?;
    encoder.write_all(data)?;
    let (compressed_data, result) = encoder.finish();
    result?;
    Ok(compressed_data)
}

fn decompress_data(data: &[u8]) -> Result<Vec<u8>, std::io::Error> {
    let mut decoder = Decoder::new(data)?;
    let mut decompressed_data = Vec::new();
    decoder.read_to_end(&mut decompressed_data)?;
    Ok(decompressed_data)
}

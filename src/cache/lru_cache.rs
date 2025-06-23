use crate::cache::compression::{compress_data, extract_data};
use chrono::{DateTime, Duration, Utc};
use lru::LruCache;
use serde_json::Value;
use std::num::NonZeroUsize;
use std::sync::{Arc, Mutex};

///Maximum entries in the cache. After the limit is reached, the oldest entries will be dropped.
const CACHE_SIZE: usize = 125;

#[derive(Debug)]
pub struct CacheEntry {
    data: Vec<u8>,
    inserted_at: DateTime<Utc>,
}

#[derive(Debug)]
pub struct LRCache {
    map: LruCache<String, CacheEntry>,
}

impl LRCache {
    pub fn create(capacity: usize) -> Self {
        LRCache {
            map: LruCache::new(NonZeroUsize::new(capacity).unwrap()),
        }
    }

    pub fn new() -> Arc<Mutex<LRCache>> {
        Arc::new(Mutex::new(LRCache::create(CACHE_SIZE)))
    }

    pub fn insert(&mut self, key: String, json: Value) {
        let json_str = serde_json::to_string(&json).unwrap();

        let compressed_data = compress_data(json_str.as_bytes()).expect("Failed to compress data");

        let entry = CacheEntry {
            data: compressed_data,
            inserted_at: Utc::now(),
        };

        self.map.put(key, entry);
    }

    pub fn get(&mut self, key: &str, expire: Duration) -> Option<Value> {
        if let Some(entry) = self.map.get_mut(key) {
            let now = Utc::now();
            let since_inserted = now
                .signed_duration_since(entry.inserted_at)
                .num_milliseconds();
            if since_inserted < expire.num_milliseconds() {
                let decompressed_data =
                    extract_data(&entry.data).expect("Failed to decompress data");
                return Some(serde_json::from_slice(&decompressed_data).unwrap());
            } else {
                self.map.pop(key);
            }
        }
        None
    }
}

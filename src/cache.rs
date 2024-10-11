use lz4::{EncoderBuilder, Decoder};
use std::io::{Read, Write};
use lru::LruCache;
use chrono::{DateTime, Utc};
use serde_json::Value;

const MAX_MEMORY_USAGE: usize = 256 * 1024 * 1024; // Set max memory usage to 256 MB
const CACHE_EXPIRATION_SECONDS: i64 = 300; // Cache expiration time in seconds

#[derive(Debug)]
struct CacheEntry {
    data: Vec<u8>,                  // Compressed JSON data
    inserted_at: DateTime<Utc>,     // Timestamp of when it was inserted
}

#[derive(Debug)]
pub struct Cache {
    map: LruCache<String, CacheEntry>, // Cache to store compressed JSON data with metadata
    current_memory_usage: usize,        // Keep track of current memory usage
}

impl Cache {
    pub fn new(capacity: usize) -> Self {
        Cache {
            map: LruCache::new(capacity),
            current_memory_usage: 0,
        }
    }

    // Compress and store JSON data
    pub fn insert(&mut self, key: String, json: &Value) {
        let json_str = serde_json::to_string(json).unwrap();

        // Compress the JSON string using LZ4
        let compressed_data = compress_data(json_str.as_bytes()).expect("Failed to compress data");

        let entry_size = compressed_data.len();

        // Check if adding this entry would exceed the max memory usage
        while self.current_memory_usage + entry_size > MAX_MEMORY_USAGE {
            self.evict(); // Evict oldest entries if necessary
        }

        // Insert new entry
        let entry = CacheEntry {
            data: compressed_data,
            inserted_at: Utc::now(),
        };
        self.map.put(key.clone(), entry);
        self.current_memory_usage += entry_size; // Update memory usage

        println!("Current memory usage: {}", self.current_memory_usage)
    }

    // Evict the oldest entry
    fn evict(&mut self) {
        if let Some((key, _)) = self.map.pop_lru() {
            if let Some(entry) = self.map.get(&key) {
                self.current_memory_usage -= entry.data.len(); // Update memory usage
            }
        }
    }

    // Get and decompress JSON data
    pub fn get(&mut self, key: &str) -> Option<Value> {
        // Check if the entry is expired
        if let Some(entry) = self.map.get_mut(key) {
            let now = Utc::now();
            if now.signed_duration_since(entry.inserted_at).num_seconds() < CACHE_EXPIRATION_SECONDS {
                // Decompress the data using LZ4
                let decompressed_data = decompress_data(&entry.data).expect("Failed to decompress data");
                return Some(serde_json::from_slice(&decompressed_data).unwrap());
            } else {
                // Entry is expired, evict it
                self.map.pop(key);
            }
        }
        None
    }
}

// Function to compress data using LZ4
fn compress_data(data: &[u8]) -> Result<Vec<u8>, std::io::Error> {
    let mut encoder = EncoderBuilder::new().build(Vec::new())?;
    encoder.write_all(data)?;
    let (compressed_data, result) = encoder.finish(); // Finish encoding and get compressed data
    result?; // Check for any errors during the encoding process
    Ok(compressed_data)
}

// Function to decompress data using LZ4
fn decompress_data(data: &[u8]) -> Result<Vec<u8>, std::io::Error> {
    let mut decoder = Decoder::new(data)?;
    let mut decompressed_data = Vec::new();
    decoder.read_to_end(&mut decompressed_data)?;
    Ok(decompressed_data)
}

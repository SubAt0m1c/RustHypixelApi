use lz4::{Decoder, EncoderBuilder};
use moka::future::Cache;
use moka::Expiry;
use serde_json::Value;
use std::io;
use std::io::{Read, Write};
use std::sync::Arc;
use std::time::{Duration, Instant};

const CACHE_SIZE: u64 = 125;

pub struct ExpireEntry {
    duration: Duration,
    value: Vec<u8>
}

impl ExpireEntry {
    pub fn new(duration: Duration, value: Vec<u8>) -> Self {
        Self {
            duration,
            value
        }
    }
}

pub struct Expire;
impl Expiry<String, Arc<ExpireEntry>> for Expire {
    fn expire_after_create(&self, _key: &String, value: &Arc<ExpireEntry>, _created_at: Instant) -> Option<Duration> {
        Some(value.duration)
    }
}

#[derive(Clone)]
pub struct MokaCache(Cache<String, Arc<ExpireEntry>>);

impl MokaCache {
    pub fn new() -> Self {
        let cache = Cache::builder()
            .max_capacity(CACHE_SIZE)
            .expire_after(Expire)
            .build();

        Self(cache)
    }

    pub async fn insert(&self, key: String, value: &Value, duration: Duration) -> Result<(), io::Error> {
        let json_str = serde_json::to_string(&value)?;
        let compressed = compress_data(json_str.as_bytes())?;

        self.0.insert(key, Arc::new(ExpireEntry::new(duration, compressed))).await;
        Ok(())
    }
    
    pub async fn get(&self, key: &str) -> Option<Value> {
        let json: Option<Value> = serde_json::from_slice(&extract_data(&self.0.get(key).await?.as_ref().value).ok()?).ok();
        json
    }
}

fn compress_data(data: &[u8]) -> Result<Vec<u8>, io::Error> {
    let mut encoder = EncoderBuilder::new().build(Vec::new())?;
    encoder.write_all(data)?;
    let (compressed, result) = encoder.finish();
    result?;
    Ok(compressed)
}

fn extract_data(data: &[u8]) -> Result<Vec<u8>, io::Error> {
    let mut result = Vec::new();
    Decoder::new(data)?.read_to_end(&mut result)?;
    Ok(result)
}
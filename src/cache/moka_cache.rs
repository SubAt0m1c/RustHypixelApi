use crate::cache::compression::{compress_data, extract_data};
use actix_web::web::Bytes;
use moka::future::Cache;
use moka::Expiry;
use std::io;
use std::sync::Arc;
use std::time::{Duration, Instant};

const CACHE_SIZE: u64 = 125;

pub type MokaEntry = Arc<ExpireEntry>;

#[derive(Clone)]
pub struct ExpireEntry {
    duration: Duration,
    value: Vec<u8>,
}

impl ExpireEntry {
    pub fn new(duration: Duration, value: Vec<u8>) -> MokaEntry {
        Arc::new(Self { duration, value })
    }
}

pub struct Expire;
impl Expiry<String, MokaEntry> for Expire {
    fn expire_after_create(
        &self,
        _key: &String,
        value: &MokaEntry,
        _created_at: Instant,
    ) -> Option<Duration> {
        Some(value.duration)
    }
}

#[derive(Clone)]
pub struct MokaCache(Cache<String, MokaEntry>);

impl MokaCache {
    pub fn new() -> Self {
        let cache = Cache::builder()
            .max_capacity(CACHE_SIZE)
            .expire_after(Expire)
            .build();

        Self(cache)
    }

    pub async fn insert(
        &self,
        key: String,
        value: Bytes,
        duration: Duration,
    ) -> Result<(), io::Error> {
        let compressed = compress_data(&value)?;

        self.0
            .insert(key, ExpireEntry::new(duration, compressed))
            .await;
        Ok(())
    }

    pub async fn get(&self, key: &str) -> Option<String> {
        let extracted = extract_data(&self.0.get(key).await?.value).ok()?;
        String::from_utf8(extracted).ok()
    }
}

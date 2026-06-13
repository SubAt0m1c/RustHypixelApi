use std::time::{Duration, Instant};

use actix_web::web::Bytes;
use moka::Expiry;

use crate::cache::cache_key::CacheKey;

#[derive(Clone)]
pub struct MemoryEntry {
    pub duration: Duration,
    pub value: Bytes,
}

impl MemoryEntry {
    pub fn new(duration: Duration, value: Bytes) -> Self {
        Self { duration, value }
    }
}

pub struct Expire;
impl Expiry<CacheKey, MemoryEntry> for Expire {
    fn expire_after_create(
        &self,
        _key: &CacheKey,
        value: &MemoryEntry,
        _created_at: Instant,
    ) -> Option<Duration> {
        Some(value.duration)
    }
}
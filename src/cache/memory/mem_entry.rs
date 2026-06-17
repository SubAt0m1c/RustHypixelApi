use std::time::{Duration, Instant};

use actix_web::web::Bytes;
use moka::Expiry;

use crate::cache::cache_key::CacheKey;

pub struct Expire;
impl Expiry<CacheKey, Bytes> for Expire {
    fn expire_after_create(
        &self,
        key: &CacheKey,
        _value: &Bytes,
        _created_at: Instant,
    ) -> Option<Duration> {
        Some(key.cache_ttl())
    }
}
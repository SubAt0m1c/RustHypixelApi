use std::time::{Duration, Instant};

use actix_web::web::Bytes;
use moka::Expiry;

use crate::cache::UuidKey;

pub mod mem_cache;

pub struct Expire;
impl Expiry<UuidKey, Bytes> for Expire {
    fn expire_after_create(
        &self,
        key: &UuidKey,
        _value: &Bytes,
        _created_at: Instant,
    ) -> Option<Duration> {
        Some(key.expires())
    }
}
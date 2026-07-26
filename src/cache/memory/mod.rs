use std::{ops::Deref, sync::{Arc, LazyLock}, time::{Duration, Instant}};

use actix_web::web::Bytes;
use moka::{Expiry, future::Cache, notification::RemovalCause};
use rapidhash_lite::RandomHash;

use crate::{cache::UuidKey, env_var, logging::{LogMessage, log}};

/// Maximum size for the cache in megabytes.
static CACHE_SIZE_MB: LazyLock<u64> = LazyLock::new(|| env_var("CACHE_SIZE_MB", 384));

#[derive(Clone)]
pub struct CacheEntry {
    data: Bytes,
    ttl: Duration,
}

impl CacheEntry {
    pub fn new(data: Bytes, ttl: Duration) -> Self {
        Self { data, ttl }
    }

    pub fn from_vec(data: Vec<u8>, ttl: Duration) -> Self {
        Self::new(Bytes::from(data), ttl)
    }

    fn len(&self) -> usize {
        self.data.len()
    }

    pub fn into_bytes(self) -> Bytes {
        self.data
    }
}

/// Thin wrapper around a Moka Cache with keys and entries already defined.
#[derive(Clone)] 
pub struct MemoryCache(Cache<UuidKey, CacheEntry, RandomHash>);

impl MemoryCache {
    pub fn new() -> Self {
        let cache = Cache::builder()
            .weigher(|_, v: &CacheEntry| v.len().try_into().unwrap_or(u32::MAX))
            .max_capacity(*CACHE_SIZE_MB * 1024 * 1024)
            .expire_after(Expire)
            .eviction_listener(|key, _, cause| {
                if matches!(cause, RemovalCause::Size) {
                    log(LogMessage::MessageAndUser { key: Arc::unwrap_or_clone(key), message: "Entry removed due to size constraints." });
                }
            })
            .build_with_hasher(RandomHash::default());

        Self(cache)
    }
}

impl Deref for MemoryCache {
    type Target = Cache<UuidKey, CacheEntry, RandomHash>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

struct Expire;
impl Expiry<UuidKey, CacheEntry> for Expire {
    fn expire_after_create(
        &self,
        _key: &UuidKey,
        value: &CacheEntry,
        _created_at: Instant,
    ) -> Option<Duration> {
        Some(value.ttl)
    }
}
use std::{env, sync::{Arc, LazyLock}, time::Duration};

use actix_web::web::Bytes;
use moka::future::Cache;

use crate::{cache::{cache_key::CacheKey, memory::mem_entry::{Expire, MemoryEntry}}, error::ProcessError};

/// Maximum size for the cache in megabytes.
static CACHE_SIZE: LazyLock<u64> = LazyLock::new(|| {
    let size = env::var("CACHE_SIZE");
    match size {
        Ok(size) => {
            size.parse().expect("CACHE_SIZE should be a a u64!")
        }
        Err(e) => {
            eprintln!("{e}: CACHE_SIZE, using 384 (mb) default.");
            384
        }
    }
});

/// Thin wrapper around a Moka Cache with keys and entries already defined.
#[derive(Clone)]
pub struct MemoryCache(Cache<CacheKey, MemoryEntry>);

impl MemoryCache {
    pub fn new() -> Self {
        let cache = Cache::builder()
            .weigher(|_, v: &MemoryEntry| v.value.len().try_into().unwrap_or(u32::MAX))
            .max_capacity(*CACHE_SIZE * 1024 * 1024)
            .expire_after(Expire)
            .build();

        Self(cache)
    }
    
    // pub async fn insert(&self, key: CacheKey, value: Bytes, duration: Duration) {
    //     self.0
    //         .insert(key, MemoryEntry::new(duration, value))
    //         .await
    // }

    pub async fn try_get_with<F, E>(&self, key: CacheKey, init: F) -> Result<Bytes, ProcessError>
    where
        F: Future<Output=Result<MemoryEntry, E>>,
        E: Send + Sync + 'static
    {
        self.0.try_get_with(key, init).await.map(|entry| entry.value).map_err(|_| ProcessError::internal("Failed try get with"))
    }
    
    // pub async fn get(&self, key: CacheKey) -> Option<Bytes> {
    //     self.0.get(&key).await.map(|entry| entry.value)
    // }
}
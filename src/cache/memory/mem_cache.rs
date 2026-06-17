use std::{env, sync::{Arc, LazyLock}, time::Duration};

use actix_web::web::Bytes;
use moka::{future::Cache, notification::RemovalCause};

use crate::{cache::{cache_key::CacheKey, memory::mem_entry::Expire}, error::ProcessError, logging::{LogMessage, log}};

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
pub struct MemoryCache(Cache<CacheKey, Bytes>);

impl MemoryCache {
    pub fn new() -> Self {
        let cache = Cache::builder()
            .weigher(|_, v: &Bytes| v.len().try_into().unwrap_or(u32::MAX)) // add eviction listener logging
            .max_capacity(*CACHE_SIZE * 1024 * 1024)
            .expire_after(Expire)
            .eviction_listener(|key, _, cause| {
                let message = match cause {
                    RemovalCause::Size => "Entry removed due to size constraints.",
                    RemovalCause::Expired => "Entry Expired",
                    _ => "Entry either replaced or deleted manually"
                };
                log(LogMessage::MessageAndUser { id: key.uuid(), message })
            })
            .build();

        Self(cache)
    }
    
    // pub async fn insert(&self, key: CacheKey, value: Bytes, duration: Duration) {
    //     self.0
    //         .insert(key, MemoryEntry::new(duration, value))
    //         .await
    // }

    pub async fn try_get_with<F>(&self, key: CacheKey, init: F) -> Result<Bytes, ProcessError>
    where
        F: Future<Output=Result<Bytes, ProcessError>>,
    {
        self.0.try_get_with(key, init).await.map_err(Arc::unwrap_or_clone)
    }
    
    // pub async fn get(&self, key: CacheKey) -> Option<Bytes> {
    //     self.0.get(&key).await.map(|entry| entry.value)
    // }
}
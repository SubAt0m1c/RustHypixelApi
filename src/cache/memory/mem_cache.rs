use std::sync::{Arc, LazyLock};

use actix_web::web::Bytes;
use moka::{future::Cache, notification::RemovalCause};

use crate::{cache::{UuidKey, memory::Expire}, error::ProcessError, logging::{LogMessage, log}, request_utils::env_var};

/// Maximum size for the cache in megabytes.
static CACHE_SIZE: LazyLock<u64> = LazyLock::new(|| env_var("CACHE_SIZE", 384));

/// Thin wrapper around a Moka Cache with keys and entries already defined.
#[derive(Clone)]
pub struct MemoryCache(Cache<UuidKey, Bytes>);

impl MemoryCache {
    pub fn new() -> Self {
        let cache = Cache::builder()
            .weigher(|_, v: &Bytes| v.len().try_into().unwrap_or(u32::MAX))
            .max_capacity(*CACHE_SIZE * 1024 * 1024)
            .expire_after(Expire)
            .eviction_listener(|key, _, cause| {
                if matches!(cause, RemovalCause::Size) {
                    log(LogMessage::MessageAndUser { key: Arc::unwrap_or_clone(key), message: "Entry removed due to size constraints." })
                }
            })
            .build();

        Self(cache)
    }

    pub async fn try_get_with<F: Future<Output=Result<Bytes, ProcessError>>>(&self, key: UuidKey, init: F) -> Result<Bytes, ProcessError> {
        self.0.try_get_with(key, init).await.map_err(Arc::unwrap_or_clone)
    }

}
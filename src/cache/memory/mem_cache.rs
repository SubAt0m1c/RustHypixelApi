use std::{env, sync::LazyLock, time::Duration};

use actix_web::web::Bytes;
use moka::future::Cache;

use crate::cache::{cache_key::CacheKey, memory::mem_entry::{Expire, MemoryEntry}};

static CACHE_SIZE: LazyLock<u64> = LazyLock::new(|| {
    let size = env::var("CACHE_SIZE");
    match size {
        Ok(size) => {
            size.parse().expect("CACHE_SIZE should be a !")
        }
        Err(e) => {
            eprintln!("Couldn't find environment variable for CACHE_SIZE, using 256 default. {e}");
            256u64
        }
    }
});

#[derive(Clone)]
pub struct MemoryCache(Cache<CacheKey, MemoryEntry>);

impl MemoryCache {
    pub fn new() -> Self {
        let cache = Cache::builder()
            .max_capacity(*CACHE_SIZE)
            .expire_after(Expire)
            .build();

        Self(cache)
    }

    pub async fn get_or_insert_with<F>(&self, key: CacheKey, init: F) -> Option<Bytes>
    where
            F: Future<Output = Option<MemoryEntry>>,
    {
        self.0.optionally_get_with(key, init).await.map(|e| e.value)
    }
    
    pub async fn insert(&self, key: CacheKey, value: Bytes, duration: Duration) {
        self.0
            .insert(key, MemoryEntry::new(duration, value))
            .await
    }

    pub async fn get(&self, key: CacheKey) -> Option<Bytes> {
        Some(self.0.get(&key).await?.value)
    }
}
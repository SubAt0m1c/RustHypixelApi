use std::time::Duration;

use actix_web::{cookie::time::UtcDateTime, web::Bytes};

use crate::{cache::{cache_key::CacheKey, compression::{compress_data, extract_data}, database::{db_entry::DbEntry, db_handle::DbHandle}, memory::{mem_cache::MemoryCache, mem_entry::MemoryEntry}}, routes::{profile::PROFILE_CACHE_TTL, secrets::SECRETS_TTL_SECONDS}};

#[derive(Clone)]
pub struct CacheRouter {
    cache: MemoryCache,
    db_handle: DbHandle,
}

impl CacheRouter {
    pub fn new() -> Self {
        Self {
            cache: MemoryCache::new(),
            db_handle: DbHandle::new(),
        }
    }

    pub async fn put(&self, key: CacheKey, data: &Bytes) {
        match key {
            CacheKey::Secrets(_) => {
                self.cache.insert(key, data.clone(), Duration::from_secs(*SECRETS_TTL_SECONDS)).await;
            }
            CacheKey::Profile(id) => {
                let compressed = Bytes::from(compress_data(&data));
                let entry = DbEntry::construct(&compressed, UtcDateTime::now());
                self.cache.insert(key, entry.data(), Duration::from_secs(*PROFILE_CACHE_TTL)).await;
                self.db_handle.write(id, entry);
            }
        }
    }

    pub async fn get(&self, key: CacheKey) -> Option<Bytes> {
        match key {
            CacheKey::Secrets(_) => self.cache.get(key).await,
            CacheKey::Profile(id) => {
                let Some(cached) = self.cache.get_or_insert_with(key, async {
                    self.db_handle.read(id).await.expect("Should have successfully gotten a response from db.").map(|bytes| {
                        MemoryEntry::new(Duration::from_secs(*PROFILE_CACHE_TTL), bytes)
                    })
                }).await else { return None };

                extract_data(&cached).ok().map(Bytes::from)
            }
        }
    }
}
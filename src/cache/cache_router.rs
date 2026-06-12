use std::time::Duration;

use actix_web::{cookie::time::UtcDateTime, web::Bytes};

use crate::{cache::{database::DbHandle, db_entry::DbEntry, moka_cache::{MokaCache, MokaKey}}, routes::{profile::PROFILE_CACHE_TTL, secrets::SECRETS_TTL_SECONDS}};

#[derive(Clone)]
pub struct CacheRouter {
    cache: MokaCache,
    db_handle: DbHandle,
}

impl CacheRouter {
    pub fn new(cache: MokaCache) -> Self {
        let db_handle = DbHandle::new();
        Self {
            cache,
            db_handle,
        }
    }

    pub async fn put(&self, key: MokaKey, data: Bytes) {
        match key {
            MokaKey::Secrets(_) => {
                self.cache.insert(key, data, Duration::from_secs(*SECRETS_TTL_SECONDS)).await;
            }
            MokaKey::Profile(id) => {
                let entry = DbEntry::construct(&data, UtcDateTime::now());
                self.cache.insert(key, entry.data(), Duration::from_secs(*PROFILE_CACHE_TTL)).await;
                self.db_handle.write(id, entry);
            }
        }
    }

    pub async fn get(&self, key: MokaKey) -> Option<Bytes> {
        if let Some(cached) = self.cache.get(key).await {
            return Some(cached)
        }
        match key {
            MokaKey::Secrets(_) => None,
            MokaKey::Profile(id) => self.db_handle.read(id).await.expect("Should have successfully gotten a response from db.")
        }
    }
}
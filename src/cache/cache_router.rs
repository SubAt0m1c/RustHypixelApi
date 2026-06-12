use std::time::Duration;

use actix_web::{cookie::time::UtcDateTime, web::Bytes};

use crate::{cache::{compression::{compress_data, extract_data}, database::DbHandle, db_entry::DbEntry, moka_cache::{ExpireEntry, MokaCache, MokaKey}}, routes::{profile::PROFILE_CACHE_TTL, secrets::SECRETS_TTL_SECONDS}};

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

    pub async fn put(&self, key: MokaKey, data: &Bytes) {
        match key {
            MokaKey::Secrets(_) => {
                self.cache.insert(key, data.clone(), Duration::from_secs(*SECRETS_TTL_SECONDS)).await;
            }
            MokaKey::Profile(id) => {
                let compressed = Bytes::from(compress_data(&data));
                let entry = DbEntry::construct(&compressed, UtcDateTime::now());
                self.cache.insert(key, entry.data(), Duration::from_secs(*PROFILE_CACHE_TTL)).await;
                self.db_handle.write(id, entry);
            }
        }
    }

    pub async fn get(&self, key: MokaKey) -> Option<Bytes> {
        match key {
            MokaKey::Secrets(_) => self.cache.get(key).await,
            MokaKey::Profile(id) => {
                let Some(cached) = self.cache.get_or_insert_with(key, async {
                    self.db_handle.read(id).await.expect("Should have successfully gotten a response from db.").map(|bytes| {
                        ExpireEntry::new(Duration::from_secs(*PROFILE_CACHE_TTL), bytes)
                    })
                }).await else { return None };

                extract_data(&cached).ok().map(Bytes::from)
            }
        }
    }
}
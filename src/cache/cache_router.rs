use actix_web::web::Bytes;

use crate::{cache::{cache_key::CacheKey, database::db_handle::DbHandle, memory::mem_cache::MemoryCache}, error::ProcessError};

/// Routes cache requests to the memory cache and db cache.
/// Secret requests/insertions will not query the db.
pub struct CacheRouter {
    cache: MemoryCache,
    database: DbHandle,
}

impl CacheRouter {
    pub fn new() -> Self {
        Self {
            cache: MemoryCache::new(),
            database: DbHandle::new(),
        }
    }

    /// Attempts to get the cache entry from the cache or fetches an entry into the cache if there is none.
    pub async fn get<K: CacheKey>(&self, key: K) -> Result<Bytes, ProcessError> {
        self.cache.try_get_with(key.key(), key.get_or_insert(&self.database)).await
    }
}
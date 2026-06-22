use actix_web::web::Bytes;

use crate::{request_utils::request, cache::{cache_key::CacheKey, database::db_handle::DbHandle, memory::mem_cache::MemoryCache}, error::ProcessError, logging::{LogMessage, log}};

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

    pub async fn get(&self, key: CacheKey, processer: fn(Bytes) -> Result<Bytes, ProcessError>) -> Result<Bytes, ProcessError> {
        // try_get_with actually handles the pending queue for us and doesnt suck at it
        self.cache.try_get_with(key, async {
            log(LogMessage::MessageAndUser {
                key,
                message: "LOADER START",
            });
            let res = Ok(match key {
                CacheKey::Profile(id) => {
                    if let Ok(Some(db_data)) = self.database.read(id).await {
                        log(LogMessage::MessageAndUser { key, message: "DB Hit" });
                        return Ok(db_data)
                    }

                    let raw = request(key, format!("https://api.hypixel.net/v2/skyblock/profiles?uuid={}", id)).await.and_then(processer)?;
                    self.database.write(id, raw.clone());
                    raw
                }
                CacheKey::Secrets(id) => request(key, format!("https://api.hypixel.net/v2/player?uuid={}", id)).await.and_then(processer)?,
            });


            log(LogMessage::MessageAndUser {
                key,
                message: "LOADER END",
            });
            res
        }).await
    }
}
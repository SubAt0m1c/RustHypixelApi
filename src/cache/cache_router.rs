use std::time::Instant;

use actix_web::web::Bytes;

use crate::{request_utils::request, cache::{cache_key::CacheKey, database::db_handle::DbHandle, memory::{mem_cache::MemoryCache, mem_entry::MemoryEntry}}, error::ProcessError, logging::{LogMessage, log}};

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

    // pub async fn put(&self, key: CacheKey, data: &Bytes) {
    //     self.cache.insert(key, data.clone(), key.cache_ttl()).await;
    //     if let CacheKey::Profile(id) = key {
    //         self.database.write(id, data.clone())
    //     }
    // }

    pub async fn get(&self, key: CacheKey, processer: fn(Bytes) -> Result<Bytes, ProcessError>) -> Result<Bytes, ProcessError> {
        // try_get_with actually handles the pending queue for us but doesnt suck at it
        self.cache.try_get_with::<_, ProcessError>(key, async {
            if let CacheKey::Profile(id) = key {
                if let Ok(Some(db_data)) = self.database.read(id).await {
                    log(LogMessage::MessageAndUser { id, message: "DB Hit" });
                    return Ok(MemoryEntry::new(key.cache_ttl(), db_data))
                }
            }

            let now = Instant::now();
            let raw = request(key.hypixel_url()).await.and_then(processer)?;
            log(LogMessage::ElapsedAndUser { id: key.uuid(), elapsed: now.elapsed(), message: "Hypixel Hit" });
            
            if let CacheKey::Profile(id) = key {
                self.database.write(id, raw.clone());
            }
            
            Ok(MemoryEntry::new(key.cache_ttl(), raw))
        }).await
        
        // if let Some(cached) = self.cache.get(key).await {
        //     log(LogMessage::ElapsedAndUser { id: key.uuid(), elapsed: start.elapsed(), message: "Cache hit" });
        //     return Some(cached)
        // }

        // let CacheKey::Profile(id) = key else { return None };
        // let now = Instant::now();
        // let res = self.database.read(id).await.expect("Should have successfully gotten a response from db.")?;
        // let db_elapsed = now.elapsed();
        // self.cache.insert(key, res.clone(), key.cache_ttl()).await;
        // log(LogMessage::DoubleElapsed { id: key.uuid(), first_elapsed: db_elapsed, second_elapsed: start.elapsed(), message: "DB hit" });
        // Some(res)
    }
}
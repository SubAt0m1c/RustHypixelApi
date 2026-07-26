use uuid::Uuid;

use crate::{cache::{UuidKey, cache_router::Database, memory::CacheEntry}, error::ProcessError, routes::stats::RateLimit};

pub trait CacheKey: Send {
    /// flag for db storage/etc. 
    /// MUST be unique across implementations of `CacheKey`.
    /// can only store a max of 8 values rn
    const KEYFLAG: u8;
    
    fn uuid(&self) -> Uuid;

    /// This function is run when this key results in a cache miss on the memory cache.
    /// If this function returns `Ok()`, it will add the Bytes into the memory cache.
    /// Otherwise, no entry will be added to the memory cache and the error should be
    /// propegated upwards.
    fn get_or_insert(&self, db: &Database, stats: &RateLimit) -> impl Future<Output = Result<CacheEntry, ProcessError>> + Send;
    
    fn key(&self) -> UuidKey {
        UuidKey::encode(self.uuid(), Self::KEYFLAG)
    }
}
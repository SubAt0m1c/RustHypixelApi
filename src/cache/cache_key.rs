use ltmdb::{Database, Runtime};
use uuid::Uuid;

use crate::{cache::{UuidKey, memory::CacheEntry}, error::ProcessError};

pub trait CacheKey {
    /// flag for db storage/etc. 
    /// MUST be unique across implementations of CacheKey.
    /// can only store a max of 8 values rn
    const KEYFLAG: u8;
    
    fn uuid(&self) -> Uuid;

    /// This function is run when this key results in a cache miss on the memory cache.
    /// If this function returns Ok(), it will add the Bytes into the memory cache.
    /// Otherwise, no entry will be added to the memory cache and the error should be
    /// propegated upwards.
    async fn get_or_insert<RT: Runtime + Send + Sync + 'static>(&self, db: &Database<RT>) -> Result<CacheEntry, ProcessError>;
    
    fn key(&self) -> UuidKey {
        UuidKey::encode(self.uuid(), Self::KEYFLAG)
    }
}
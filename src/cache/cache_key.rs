use actix_web::web::Bytes;
use uuid::Uuid;

use crate::{cache::{UuidKey, database::db_handle::DbHandle, expires::Expires}, error::ProcessError};

pub trait CacheKey {
    const KEYFLAG: u8;
    
    fn uuid(&self) -> Uuid;
    
    fn expires(&self) -> Expires;

    /// Ran when this key results in a cache miss on the memory cache.
    /// If this function returns Ok(), it will add the Bytes into the memory cache.
    /// Otherwise, no entry will be added to the memory cache and the error should be
    /// propegated up.
    async fn get_or_insert(&self, db: &DbHandle) -> Result<Bytes, ProcessError>;
    
    fn key(&self) -> UuidKey {
        UuidKey::encode(self.uuid(), Self::KEYFLAG, self.expires())
    }
}
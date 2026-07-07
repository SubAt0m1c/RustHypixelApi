use std::{str::FromStr, sync::LazyLock, time::{Duration, Instant}};

use actix_web::{error::ErrorInternalServerError, get, web::{Data, Path}, Responder};
use ltmdb::{Database, Runtime};
use uuid::Uuid;

use crate::{cache::{cache_key::CacheKey, cache_router::CacheRouter, compression::{compress_data, extract_data}, memory::CacheEntry}, error::ProcessError, logging::{LogMessage, log}, request_utils::{env_var, json_response, request}};

/// Database time to live for profile queries in seconds.
pub static PROFILE_DB_TTL_SECONDS: LazyLock<Duration> = LazyLock::new(|| Duration::from_secs(env_var("PROFILE_DB_TTL_SECONDS", 120)));
/// Cache time to live for profile queries in seconds.
pub static PROFILE_CACHE_TTL_SECONDS: LazyLock<Duration> = LazyLock::new(|| Duration::from_secs(env_var("PROFILE_CACHE_TTL_SECONDS", 120)));

struct ProfileKey(Uuid);

impl CacheKey for ProfileKey {
    const KEYFLAG: u8 = 0;

    fn uuid(&self) -> Uuid {
        self.0
    }
 
    async fn get_or_insert<RT: Runtime + Send + Sync + 'static>(&self, db: &Database<RT>) -> Result<CacheEntry, ProcessError> {
        let uuid_key = self.key();
        let now = Instant::now();
        let bytes = db.read(uuid_key).await?.map(|b| extract_data(&b)).transpose().map_err(|e| ProcessError::DatabaseError(e.to_string()))?;
        log(LogMessage::TimeElapsed { elapsed: now.elapsed(), name: "DB Read" });
        
        if let Some(db_data) = bytes {
            log(LogMessage::MessageAndUser { key: uuid_key, message: "DB Hit" });
            return Ok(CacheEntry::from_vec(db_data, *PROFILE_CACHE_TTL_SECONDS))
        }
    
        let bytes = request(uuid_key, format!("https://api.hypixel.net/v2/skyblock/profiles?uuid={}", self.uuid())).await?;

        let now = Instant::now();
        db.insert(uuid_key, compress_data(&bytes), *PROFILE_DB_TTL_SECONDS).await?;
        log(LogMessage::TimeElapsed { elapsed: now.elapsed(), name: "DB write" });
        
        Ok(CacheEntry::new(bytes, *PROFILE_CACHE_TTL_SECONDS))
    }
}

#[get("/get/{uuid}")]
async fn profile(
    path: Path<String>,
    cache: Data<CacheRouter>,
) -> actix_web::Result<impl Responder> {
    let uuid = Uuid::from_str(&path.into_inner()).map_err(ErrorInternalServerError)?;
    let data = cache.get(ProfileKey(uuid)).await?;
    Ok(json_response(data))
}

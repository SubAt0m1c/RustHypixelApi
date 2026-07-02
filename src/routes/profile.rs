use std::{str::FromStr, sync::LazyLock, time::{Duration, Instant}};

use actix_web::{error::ErrorInternalServerError, get, web::{Bytes, Data, Path}, Responder};
use ltmdb::{Database, Runtime};
use uuid::Uuid;

use crate::{cache::{cache_key::CacheKey, cache_router::CacheRouter, compression::{compress_data, extract_data}, expires::Expires}, error::ProcessError, logging::{log, LogMessage}, request_utils::{env_var, json_response, request}};

/// Database time to live for profile queries in seconds.
pub static PROFILE_DB_TTL: LazyLock<Duration> = LazyLock::new(|| Duration::from_secs(env_var("PROFILE_DB_TTL", 3600)));
/// Cache time to live for profile queries in seconds.
pub static PROFILE_CACHE_TTL: LazyLock<u8> = LazyLock::new(|| env_var("PROFILE_CACHE_TTL", 120));

struct ProfileKey(Uuid);

impl CacheKey for ProfileKey {
    const KEYFLAG: u8 = 0;

    fn uuid(&self) -> Uuid {
        self.0
    }

    fn expires(&self) -> Expires {
        Expires::new(*PROFILE_CACHE_TTL)
    }

    async fn get_or_insert<RT: Runtime + Send + Sync + 'static>(&self, db: &Database<RT>) -> Result<actix_web::web::Bytes, crate::error::ProcessError> {
        let uuid_key = self.key();
        let key = uuid_key.as_u128();
        let now = Instant::now();
        let bytes = db.read(key).await?.map(|b| extract_data(&b)).transpose().map_err(|e| ProcessError::DatabaseError(e.to_string()))?;
        log(LogMessage::TimeElapsed { elapsed: now.elapsed(), name: "DB Read" });
        
        if let Some(db_data) = bytes {
            log(LogMessage::MessageAndUser { key: uuid_key, message: "DB Hit" });
            return Ok(Bytes::from(db_data))
        }
    
        let bytes = request(uuid_key, format!("https://api.hypixel.net/v2/skyblock/profiles?uuid={}", self.uuid())).await?;

        let now = Instant::now();
        db.insert(key, compress_data(&bytes), *PROFILE_DB_TTL).await?;
        log(LogMessage::TimeElapsed { elapsed: now.elapsed(), name: "DB write" });
        
        Ok(bytes)
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

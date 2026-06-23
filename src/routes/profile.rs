use std::{str::FromStr, sync::LazyLock, time::Duration};

use actix_web::{Responder, error::ErrorInternalServerError, get, web::{Data, Path}};
use uuid::Uuid;

use crate::{cache::{cache_key::CacheKey, cache_router::CacheRouter, database::db_handle::DbHandle, expires::Expires}, logging::{LogMessage, log}, request_utils::{env_var, json_response, request}};

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

    async fn get_or_insert(&self, db: &DbHandle) -> Result<actix_web::web::Bytes, crate::error::ProcessError> {
        let key = self.key();
        if let Ok(Some(db_data)) = db.read(key).await {
            log(LogMessage::MessageAndUser { key: key, message: "DB Hit" });
            return Ok(db_data)
        }
    
        request(key, format!("https://api.hypixel.net/v2/skyblock/profiles?uuid={}", self.uuid())).await
            .inspect(|b| db.write(key, *PROFILE_DB_TTL, b.clone()))
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

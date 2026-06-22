use crate::cache::cache_key::CacheKey;
use crate::cache::cache_router::CacheRouter;
use crate::request_utils::{env_var, json_response};
use actix_web::error::ErrorInternalServerError;
use actix_web::web::{Data, Path};
use actix_web::{get, Responder};
use uuid::Uuid;
use std::str::FromStr;
use std::sync::LazyLock;

/// Database time to live for profile queries in seconds.
pub static PROFILE_DB_TTL: LazyLock<u64> = LazyLock::new(|| env_var("PROFILE_DB_TTL", 3600));
/// Cache time to live for profile queries in seconds.
pub static PROFILE_CACHE_TTL: LazyLock<u64> = LazyLock::new(|| env_var("PROFILE_CACHE_TTL", 120));

#[get("/get/{uuid}")]
async fn profile(
    path: Path<String>,
    cache: Data<CacheRouter>,
) -> actix_web::Result<impl Responder> {
    let uuid = Uuid::from_str(&path.into_inner()).map_err(ErrorInternalServerError)?;
    let cache_key = CacheKey::Profile(uuid);

    let data = cache.get(cache_key, |bytes| {
        Ok(bytes)
    }).await?;

    Ok(json_response(data))
}

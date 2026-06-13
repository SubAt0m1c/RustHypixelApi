use crate::api_handler::ApiHandler;
use crate::cache::cache_key::CacheKey;
use crate::logging::{LogMessage, log};
use crate::utils::json_response;
use actix_web::error::ErrorInternalServerError;
use actix_web::web::{Data, Path};
use actix_web::{get, Responder};
use reqwest::Client;
use uuid::Uuid;
use std::env;
use std::str::FromStr;
use std::sync::LazyLock;

pub static PROFILE_DB_TTL: LazyLock<u64> = LazyLock::new(|| {
    let size = env::var("PROFILE_TTL_SECONDS");
    match size {
        Ok(size) => {
            size.parse().expect("PROFILE_TTL_SECONDS should be a u64!")
        }
        Err(e) => {
            eprintln!("Couldn't find environment variable for PROFILE_TTL_SECONDS, using 3600 (60 minutes) default. {e}");
            3600
        }
    }
});

pub static PROFILE_CACHE_TTL: LazyLock<u64> = LazyLock::new(|| {
    let size = env::var("PROFILE_CACHE_TTL");
    match size {
        Ok(size) => {
            size.parse().expect("PROFILE_CACHE_TTL should be a u64!")
        }
        Err(e) => {
            eprintln!("Couldn't find environment variable for PROFILE_CACHE_TTL, using 120 (2 minutes) default. {e}");
            120
        }
    }
});

#[get("/get/{uuid}")]
async fn profile(
    path: Path<String>,
    client: Data<Client>,
    cache: Data<ApiHandler>,
) -> actix_web::Result<impl Responder> {
    let uuid = Uuid::from_str(&path.into_inner()).map_err(ErrorInternalServerError)?;
    log(LogMessage::MessageAndUser { id: uuid, message: "Requesting profile data for user" });
    let cache_key = CacheKey::Profile(uuid);

    let data = cache.get(cache_key, client, |bytes| {
        Ok(bytes)
    }).await?;
    
    Ok(json_response(data))
}

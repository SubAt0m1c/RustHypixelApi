use crate::cache::moka_cache::{MokaCache, MokaKey};
use crate::api_handler::ApiHandler;
use crate::logging::{LogMessage, log};
use crate::utils::{fetch, json_response};
use actix_web::error::ErrorInternalServerError;
use actix_web::web::{Data, Path};
use actix_web::{get, Responder};
use reqwest::Client;
use uuid::Uuid;
use std::env;
use std::str::FromStr;
use std::sync::LazyLock;

pub static PROFILE_TTL_SECONDS: LazyLock<u64> = LazyLock::new(|| {
    let size = env::var("PROFILE_TTL_SECONDS");
    match size {
        Ok(size) => {
            size.parse().expect("PROFILE_TTL_SECONDS should be a u64!")
        }
        Err(e) => {
            eprintln!("Couldn't find environment variable for PROFILE_TTL_SECONDS, using 600 default. {e}");
            600u64
        }
    }
});

/// this is the ttl divider. I.e. if secrets have a cache time of 60 seconds with a TTL_DIV of 10,
/// it will be memory cached with a ttl of 6 seconds and database cached for all 60.
pub static TTL_DIV: LazyLock<u64> = LazyLock::new(|| {
    let size = env::var("TTL_DIV");
    match size {
        Ok(size) => {
            size.parse().expect("TTL_DIV should be a u64!")
        }
        Err(e) => {
            eprintln!("Couldn't find environment variable for TTL_DIV, using 5 default. {e}");
            5
        }
    }
});

pub static PROFILE_CACHE_TTL: LazyLock<u64> = LazyLock::new(|| {
    *PROFILE_TTL_SECONDS / *TTL_DIV
});

#[get("/get/{uuid}")]
async fn profile(
    path: Path<String>,
    client: Data<Client>,
    cache: Data<ApiHandler>,
) -> actix_web::Result<impl Responder> {
    let uuid = Uuid::from_str(&path.into_inner()).map_err(ErrorInternalServerError)?;
    log(LogMessage::MessageAndUser { id: uuid, message: "Requesting profile data for user" });
    let cache_key = MokaKey::Profile(uuid);

    let data = cache.get(cache_key, client, |bytes| {
        Some(bytes)
    }).await.ok_or(ErrorInternalServerError("Failed somewhere trying to get profile data"))?;
    
    Ok(json_response(data))
}

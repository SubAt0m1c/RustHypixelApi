use crate::cache::moka_cache::MokaCache;
use crate::utils::{fetch, json_response};
use actix_web::error::ErrorInternalServerError;
use actix_web::web::{Data, Path};
use actix_web::{get, Responder};
use reqwest::Client;
use std::time::Duration;

#[get("/get/{uuid}")]
async fn profile(
    path: Path<String>,
    client: Data<Client>,
    cache: Data<MokaCache>,
) -> actix_web::Result<impl Responder> {
    let uuid = path.into_inner().replace("-", "");
    let url = format!("https://api.hypixel.net/v2/skyblock/profiles?uuid={}", uuid);
    let cache_key = format!("{} by P", uuid);

    if let Some(cached) = cache.get(&cache_key).await {
        return Ok(json_response(cached));
    }

    let bytes = fetch(url, &client)
        .await
        .map_err(ErrorInternalServerError)?;
    cache
        .insert(cache_key, bytes.clone(), Duration::from_secs(600))
        .await;

    Ok(json_response(bytes))
}

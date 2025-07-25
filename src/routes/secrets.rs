use crate::cache::moka_cache::MokaCache;
use crate::utils::{fetch, json_response};
use actix_web::error::ErrorInternalServerError;
use actix_web::web::{Bytes, Data, Path};
use actix_web::{get, HttpResponse, Responder};
use reqwest::Client;
use serde_json::{from_slice, json, to_vec, Value};
use std::time::Duration;

#[get("/secrets/{uuid}")]
async fn secrets(
    path: Path<String>,
    client: Data<Client>,
    cache: Data<MokaCache>,
) -> actix_web::Result<impl Responder> {
    let uuid = path.into_inner().replace("-", "");
    let url = format!("https://api.hypixel.net/v2/player?uuid={}", uuid);
    let cache_key = format!("{} by S", uuid);

    if let Some(cached) = cache.get(&cache_key).await {
        return Ok(json_response(cached));
    }

    let bytes = fetch(url, &client)
        .await
        .map_err(ErrorInternalServerError)?;
    let formatted = find_secrets(from_slice::<Value>(&bytes).map_err(ErrorInternalServerError)?);
    let byte_vec = to_vec(&formatted).map_err(ErrorInternalServerError)?;

    cache
        .insert(cache_key, Bytes::from(byte_vec), Duration::from_secs(60))
        .await;

    Ok(HttpResponse::Ok().json(formatted))
}

fn find_secrets(data: Value) -> Value {
    data.get("player")
        .and_then(|player| player.get("achievements"))
        .and_then(|achievements| achievements.get("skyblock_treasure_hunter"))
        .cloned()
        .unwrap_or_else(|| json!("Couldnt find secret field in achievement data..."))
}

use crate::cache::cache_enum::CacheEnum;
use crate::utils::{fetch, json_response};
use actix_web::error::ErrorInternalServerError;
use actix_web::web::Bytes;
use actix_web::{get, web, HttpResponse, Responder};
use reqwest::Client;
use serde_json::{from_slice, json, to_vec, Value};
use std::time::Duration;

#[get("/secrets/{uuid}")]
async fn secrets(
    path: web::Path<String>,
    client: web::Data<Client>,
    cache: web::Data<CacheEnum>,
) -> actix_web::Result<impl Responder> {
    let uuid = path.into_inner().replace("-", "");
    let url = format!("https://api.hypixel.net/v2/player?uuid={}", uuid);
    let cache_key = format!("{} by S", uuid);

    if let Some(cached) = cache.get(&cache_key, Duration::from_secs(600)).await {
        return Ok(json_response(cached));
    }

    let bytes = fetch(url, client).await.map_err(ErrorInternalServerError)?;
    let formatted = format_secrets(from_slice::<Value>(&bytes).map_err(ErrorInternalServerError)?);
    let byte_vec = to_vec(&formatted).map_err(ErrorInternalServerError)?;

    cache
        .insert(cache_key, Bytes::from(byte_vec), Duration::from_secs(600))
        .await;

    Ok(HttpResponse::Ok().json(formatted))
}

fn format_secrets(data: Value) -> Value {
    data.get("player")
        .and_then(|player| player.get("achievements"))
        .and_then(|achievements| achievements.get("skyblock_treasure_hunter"))
        .cloned()
        .unwrap_or_else(|| json!("Couldnt find secret field in achievement data..."))
}

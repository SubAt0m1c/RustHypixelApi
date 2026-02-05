use crate::cache::moka_cache::{MokaCache, MokaKey};
use crate::utils::{fetch, json_response};
use actix_web::error::ErrorInternalServerError;
use actix_web::web::{Bytes, Data, Path};
use actix_web::{get, HttpResponse, Responder};
use reqwest::Client;
use serde_json::to_vec;
use simd_json::{BorrowedValue, to_borrowed_value};
use simd_json::derived::ValueObjectAccess;
use uuid::Uuid;
use std::str::FromStr;
use std::time::Duration;

#[get("/secrets/{uuid}")]
async fn secrets(
    path: Path<String>,
    client: Data<Client>,
    cache: Data<MokaCache>,
) -> actix_web::Result<impl Responder> {
    let uuid = Uuid::from_str(&path.into_inner()).map_err(ErrorInternalServerError)?;
    let url = format!("https://api.hypixel.net/v2/player?uuid={}", uuid);
    let cache_key = MokaKey::Secrets(uuid);

    if let Some(cached) = cache.get(cache_key).await {
        return Ok(json_response(cached));
    }

    let mut bytes: Vec<u8> = fetch(url, &client)
        .await
        .map_err(ErrorInternalServerError)?
        .to_vec();
    
    let json = to_borrowed_value(&mut bytes).map_err(ErrorInternalServerError)?;
    let formatted = find_secrets(&json).ok_or(ErrorInternalServerError("Failed to find secrets"))?;
    let byte_vec = to_vec(&formatted).map_err(ErrorInternalServerError)?;

    cache
        .insert(cache_key, Bytes::from(byte_vec), Duration::from_secs(60))
        .await;

    Ok(HttpResponse::Ok().json(formatted))
}

fn find_secrets<'a>(data: &'a BorrowedValue<'a>) -> Option<&'a BorrowedValue<'a>> {
    let res = data.get("player")
        .and_then(|player| player.get("achievements"))
        .and_then(|achievements| achievements.get("skyblock_treasure_hunter"));
    res
}

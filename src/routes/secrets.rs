use crate::cache::moka_cache::{MokaCache, MokaKey};
use crate::api_handler::ApiHandler;
use crate::logging::{LogMessage, log};
use crate::utils::{fetch, json_response};
use actix_web::error::ErrorInternalServerError;
use actix_web::web::{Bytes, Data, Path};
use actix_web::{get, HttpResponse, Responder};
use reqwest::Client;
use serde_json::to_vec;
use simd_json::{BorrowedValue, to_borrowed_value};
use simd_json::derived::ValueObjectAccess;
use uuid::Uuid;
use std::env;
use std::str::FromStr;
use std::sync::LazyLock;

pub static SECRETS_TTL_SECONDS: LazyLock<u64> = LazyLock::new(|| {
    let size = env::var("SECRETS_TTL_SECONDS");
    match size {
        Ok(size) => {
            size.parse().expect("SECRETS_TTL_SECONDS should be a u64!")
        }
        Err(e) => {
            eprintln!("Couldn't find environment variable for SECRETS_TTL_SECONDS, using 120 default. {e}");
            120
        }
    }
});

#[get("/secrets/{uuid}")]
async fn secrets(
    path: Path<String>,
    client: Data<Client>,
    cache: Data<ApiHandler>,
) -> actix_web::Result<impl Responder> {
    let uuid = Uuid::from_str(&path.into_inner()).map_err(ErrorInternalServerError)?;
    log(LogMessage::MessageAndUser { id: uuid, message: "Requesting secret data for user" });
    let cache_key = MokaKey::Secrets(uuid);

    let data = cache.get(cache_key, client, |bytes| {
        let mut vec = bytes.to_vec(); // im cryin
        let json = to_borrowed_value(&mut vec).ok()?;
        let formatted = find_secrets(&json)?;
        Some(Bytes::from(to_vec(formatted).ok()?))
    }).await.ok_or(ErrorInternalServerError("Failed somewhere trying to get secret data."))?;

    Ok(json_response(data))
}

fn find_secrets<'a>(data: &'a BorrowedValue<'a>) -> Option<&'a BorrowedValue<'a>> {
    let res = data.get("player")
        .and_then(|player| player.get("achievements"))
        .and_then(|achievements| achievements.get("skyblock_treasure_hunter"));
    res
}

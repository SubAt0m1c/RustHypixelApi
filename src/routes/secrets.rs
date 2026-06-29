use crate::cache::cache_key::CacheKey;
use crate::cache::cache_router::{CacheRouter, TokioRT};
use crate::cache::expires::Expires;
use crate::error::ProcessError;
use crate::request_utils::{env_var, json_response, request};
use actix_web::error::ErrorInternalServerError;
use actix_web::web::{Bytes, BytesMut, Data, Path};
use actix_web::{get, Responder};
use database::cache::Database;
use database::runtime::Runtime;
use serde_json::to_vec;
use simd_json::{BorrowedValue, to_borrowed_value};
use simd_json::derived::ValueObjectAccess;
use uuid::Uuid;
use std::str::FromStr;
use std::sync::LazyLock;

/// Cache time to live for secret queries in seconds. Secret queries do not query the database.
pub static SECRETS_TTL_SECONDS: LazyLock<u8> = LazyLock::new(|| env_var("SECRETS_TTL_SECONDS", 120));

struct SecretsKey(Uuid);

impl CacheKey for SecretsKey {
    const KEYFLAG: u8 = 1;

    fn uuid(&self) -> Uuid {
        self.0
    }

    fn expires(&self) -> Expires {
        Expires::new(*SECRETS_TTL_SECONDS)
    }

    async fn get_or_insert<RT: Runtime + Send + Sync + 'static>(&self, _: &Database<RT>) -> Result<Bytes, ProcessError> {
        request(self.key(), format!("https://api.hypixel.net/v2/player?uuid={}", self.uuid())).await.and_then(|bytes| {
            let mut vec = BytesMut::from(bytes); // theoretically this doesnt copy since reqwest makes a new bytes? not sure.
            let json = to_borrowed_value(&mut vec)?;
            let formatted = &find_secrets(&json).ok_or(ProcessError::internal("Could not find secrets."))?;
            Ok(Bytes::from(to_vec(formatted)?))
        })
    }
}

#[get("/secrets/{uuid}")]
async fn secrets(
    path: Path<String>,
    cache: Data<CacheRouter<TokioRT>>,
) -> actix_web::Result<impl Responder> {
    let uuid = Uuid::from_str(&path.into_inner()).map_err(ErrorInternalServerError)?;
    let data = cache.get(SecretsKey(uuid)).await?;

    Ok(json_response(data))
}

/// Extracts the secret field from hypixel's achievement data. The data in the profile fields is per-profile and takes longer to update.
fn find_secrets<'a>(data: &'a BorrowedValue<'a>) -> Option<&'a BorrowedValue<'a>> {
    let res = data.get("player")
        .and_then(|player| player.get("achievements"))
        .and_then(|achievements| achievements.get("skyblock_treasure_hunter"));
    res
}

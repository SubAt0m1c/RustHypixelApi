use std::{str::FromStr, sync::LazyLock, time::Duration};

use actix_web::{Responder, error::ErrorInternalServerError, get, web::{BytesMut, Data, Path}};
use ltmdb::{Database, Runtime};
use serde_json::to_vec;
use simd_json::{BorrowedValue, derived::ValueObjectAccess, to_borrowed_value};
use uuid::Uuid;

use crate::{cache::{cache_key::CacheKey, cache_router::CacheRouter, memory::CacheEntry}, env_var, error::ProcessError, request_utils::{json_response, request}};

/// Cache time to live for secret queries in seconds. Secret queries do not query the database.
pub static SECRETS_TTL_SECONDS: LazyLock<Duration> = LazyLock::new(|| Duration::from_secs(env_var("SECRETS_TTL_SECONDS", 120)));

struct SecretsKey(Uuid);

impl CacheKey for SecretsKey {
    const KEYFLAG: u8 = 1;

    fn uuid(&self) -> Uuid {
        self.0
    }

    async fn get_or_insert<RT: Runtime + Send + Sync + 'static>(&self, _: &Database<RT>) -> Result<CacheEntry, ProcessError> {
        request(self.key(), format!("https://api.hypixel.net/v2/player?uuid={}", self.uuid())).await.and_then(|bytes| {
            let mut vec = BytesMut::from(bytes); // theoretically this doesnt copy since reqwest makes a new bytes? not sure.
            let json = to_borrowed_value(&mut vec)?;
            let formatted = &find_secrets(&json).ok_or(ProcessError::internal("Could not find secrets."))?;
            Ok(CacheEntry::from_vec(to_vec(formatted)?, *SECRETS_TTL_SECONDS))
        })
    }
}

#[get("/secrets/{uuid}")]
async fn secrets(
    path: Path<String>,
    cache: Data<CacheRouter>,
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

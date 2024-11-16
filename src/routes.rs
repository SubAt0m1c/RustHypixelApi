use crate::format::format_numbers;
use crate::rate_limit::RateLimiter;
use crate::SharedCache;
use chrono::{Duration, Utc};
use reqwest::header::{HeaderMap, HeaderValue, CONTENT_TYPE};
use rocket::http::Status;
use rocket::serde::json::{json, Json};
use rocket::{get, State};
use serde_json::Value;

#[get("/<uuid>")]
pub async fn handle_players(
    _guard: RateLimiter,
    uuid: &str,
    api_key: &State<String>,
    cache: &State<SharedCache>,
) -> Result<Json<Value>, (Status, Json<Value>)> {
    match fetch_and_cache(
        &uuid.replace("-", ""),
        api_key,
        &cache,
        Duration::minutes(5),
    )
    .await
    {
        Ok(data) => Ok(Json(data)),
        Err((status, error)) => Err((status, Json(error))),
    }
}

#[get("/<uuid>")]
pub async fn handle_secrets(
    _guard: RateLimiter,
    uuid: &str,
    api_key: &State<String>,
    cache: &State<SharedCache>,
) -> Result<Json<Value>, (Status, Json<Value>)> {
    let fixed_uuid = &uuid.replace("-", "");
    match fetch_and_cache(fixed_uuid, api_key, &cache, Duration::minutes(1)).await {
        Ok(data) => Ok(Json(find_secrets(&data, fixed_uuid))),
        Err((status, error)) => Err((status, Json(error))),
    }
}

pub async fn fetch_and_cache<'a>(
    uuid: &str,
    api_key: &State<String>,
    cache: &State<SharedCache>,
    cache_duration: Duration,
) -> Result<Value, (Status, Value)> {
    let start_time = Utc::now();

    {
        let mut cache_lock = cache.lock().unwrap();

        if let Some(cached_json) = cache_lock.get(uuid, cache_duration) {
            println!(
                "Cached response time for UUID {}: {} seconds",
                uuid,
                Utc::now()
                    .signed_duration_since(start_time)
                    .num_milliseconds() as f64
                    / 1000.0
            );
            return Ok(cached_json);
        }
    }

    let client = reqwest::Client::new();
    let mut headers = HeaderMap::new();
    headers.insert(
        CONTENT_TYPE,
        HeaderValue::from_str("application/json").unwrap(),
    );
    headers.insert("API-Key", HeaderValue::from_str(api_key).unwrap());

    let url = format!("https://api.hypixel.net/v2/skyblock/profiles?uuid={}", uuid);

    match client.get(&url).headers(headers).send().await {
        Ok(response) => {
            if response.status().is_success() {
                let response_text = response.text().await.unwrap();

                let json_body = serde_json::from_str::<Value>(&response_text).unwrap();

                let formatted_json = format_numbers(&json_body);

                {
                    let mut cache_lock = cache.lock().unwrap();
                    cache_lock.insert(uuid.to_string(), &formatted_json);
                }

                let duration = Utc::now().signed_duration_since(start_time);
                println!(
                    "Response time for UUID {}: {} seconds",
                    uuid,
                    duration.num_milliseconds() as f64 / 1000.0
                );

                Ok(formatted_json)
            } else {
                let duration = Utc::now().signed_duration_since(start_time);
                println!(
                    "Failed Response time for UUID {}: {} seconds",
                    uuid,
                    duration.num_milliseconds() as f64 / 1000.0
                );

                Err((
                    Status::from_code(response.status().as_u16())
                        .unwrap_or_else(|| Status::InternalServerError),
                    serde_json::json!({
                        "error": format!("Request failed with status: {}", response.status())
                    }),
                ))
            }
        }
        Err(e) => {
            let duration = Utc::now().signed_duration_since(start_time);
            println!(
                "Failed Response time for UUID {}: {} seconds",
                uuid,
                duration.num_milliseconds() as f64 / 1000.0
            );

            Err((
                Status::InternalServerError,
                serde_json::json!({
                    "error": format!("Failed to connect to external server: {}", e)
                }),
            ))
        }
    }
}

fn find_secrets(data: &Value, uuid: &str) -> Value {
    data.get("profiles")
        .and_then(|v| v.as_array())
        .and_then(|profiles| {
            profiles
                .iter()
                .find(|profile| profile.get("selected").and_then(|v| v.as_bool()) == Some(true))
        })
        .and_then(|selected_profile| selected_profile.get("members").and_then(|v| v.as_object()))
        .and_then(|members| members.get(&uuid.replace("-", "")))
        .and_then(|player_data| player_data.get("dungeons"))
        .and_then(|dungeons| dungeons.get("secrets"))
        .cloned()
        .unwrap_or_else(|| json!(-1))
}

use crate::format::{format_numbers, format_secrets};
use crate::rate_limit::RateLimiter;
use crate::rate_tracker::RateTracker;
use crate::SharedCache;
use chrono::Duration;
use reqwest::Client;
use rocket::http::Status;
use rocket::serde::json::{json, Json};
use rocket::{get, State};
use serde_json::Value;
use std::time::Instant;

#[get("/<uuid>")]
pub async fn handle_players(
    _guard: RateLimiter,
    uuid: &str,
    client: &State<Client>,
    cache: &State<SharedCache>,
    //rate_tracker: &State<RateTracker>,
) -> Result<Json<Value>, (Status, Json<Value>)> {
    let fixed_uuid = &uuid.replace("-", "");
    let url = format!(
        "https://api.hypixel.net/v2/skyblock/profiles?uuid={}",
        fixed_uuid
    );
    let cache_id = format!("{} by P", fixed_uuid);

    match fetch_and_cache(
        &url,
        &cache_id,
        &client,
        &cache,
        //&rate_tracker,
        Duration::minutes(5),
        format_numbers,
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
    client: &State<Client>,
    cache: &State<SharedCache>,
    //rate_tracker: &State<RateTracker>,
) -> Result<Json<Value>, (Status, Json<Value>)> {
    let fixed_uuid = &uuid.replace("-", "");
    let url = format!("https://api.hypixel.net/v2/player?uuid={}", fixed_uuid);
    let cache_id = format!("{} by S", fixed_uuid);

    match fetch_and_cache(
        &url,
        &cache_id,
        &client,
        &cache,
        //&rate_tracker,
        Duration::minutes(1),
        format_secrets,
    )
    .await
    {
        Ok(data) => {
            println!("Pushed data: {}", data);
            Ok(Json(data))
        }
        Err((status, error)) => Err((status, Json(error))),
    }
}

pub async fn fetch_and_cache(
    url: &str,
    cache_id: &str,
    client: &State<Client>,
    cache: &State<SharedCache>,
    //rate_tracker: &State<RateTracker>,
    cache_duration: Duration,
    cache_format: impl FnOnce(&Value) -> Value,
) -> Result<Value, (Status, Value)> {
    let start_time = Instant::now();

    {
        let mut cache_lock = cache.lock().unwrap();

        if let Some(cached_json) = cache_lock.get(cache_id, cache_duration) {
            println!(
                "Cached response time for UUID {}: {} seconds",
                cache_id,
                start_time.elapsed().as_millis() as f64 / 1000.0
            );
            return Ok(cached_json);
        }
    }

    // {
    //     rate_tracker.inc(&start_time).await;
    //
    //     println!(
    //         "Hypixel request #{} since {}s ago",
    //         rate_tracker.requests(),
    //         rate_tracker.elapsed().await.as_secs()
    //     )
    // }

    match client.get(url).send().await {
        Ok(response) => {
            if response.status().is_success() {
                let response_text = response.text().await.unwrap();
                let json_body = serde_json::from_str::<Value>(&response_text).unwrap();
                let formatted_json = cache_format(&json_body);

                {
                    let mut cache_lock = cache.lock().unwrap();
                    cache_lock.insert(cache_id.to_string(), &formatted_json);
                }

                println!(
                    "Response time for UUID {}: {} seconds",
                    cache_id,
                    start_time.elapsed().as_millis() as f64 / 1000.0
                );

                Ok(formatted_json)
            } else {
                println!(
                    "Failed (Error: {}) Response time for UUID {}: {} seconds",
                    response.status().canonical_reason().unwrap_or_default(),
                    cache_id,
                    start_time.elapsed().as_millis() as f64 / 1000.0
                );

                Err((
                    Status::from_code(response.status().as_u16())
                        .unwrap_or_else(|| Status::InternalServerError),
                    json!({
                        "error": format!("Request failed with status: {}", response.status())
                    }),
                ))
            }
        }
        Err(e) => {
            println!(
                "Failed (Error: Failed to connect to external server!) Response time for UUID {}: {} seconds",
                cache_id,
                start_time.elapsed().as_millis() as f64 / 1000.0
            );

            Err((
                Status::InternalServerError,
                json!({
                    "error": format!("Failed to connect to external server: {}", e)
                }),
            ))
        }
    }
}

use crate::format::format_numbers;
use crate::rate_limit::RateLimiter;
use crate::SharedCache;
use chrono::Utc;
use reqwest::header::{HeaderMap, HeaderValue, CONTENT_TYPE};
use rocket::get;
use rocket::http::Status;
use rocket::serde::json::Json;
use serde_json::Value;

#[get("/<uuid>")]
pub async fn handle_players(
    _guard: RateLimiter,
    uuid: &str,
    api_key: &rocket::State<String>,
    cache: &rocket::State<SharedCache>,
) -> Result<Json<Value>, (Status, Json<Value>)> {
    let start_time = Utc::now();

    {
        let mut cache_lock = cache.lock().unwrap();

        if let Some(cached_json) = cache_lock.get(uuid) {
            println!("Using cached data!");
            let duration = Utc::now().signed_duration_since(start_time);
            println!(
                "Response time for UUID {}: {} seconds",
                uuid,
                duration.num_milliseconds() as f64 / 1000.0
            );
            return Ok(Json(cached_json));
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

                Ok(Json(formatted_json))
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
                    Json(serde_json::json!({
                        "error": format!("Request failed with status: {}", response.status())
                    })),
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
                Json(serde_json::json!({
                    "error": format!("Failed to connect to external server: {}", e)
                })),
            ))
        }
    }
}

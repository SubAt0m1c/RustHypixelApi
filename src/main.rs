mod cache;
mod format;

use std::sync::{Arc, Mutex};
use rocket::{get, routes, serde::json::Json};
use reqwest::header::{HeaderMap, HeaderValue, CONTENT_TYPE};
use serde_json::Value;
use chrono::Utc;

use cache::{Cache};
use format::format_numbers;

type SharedCache = Arc<Mutex<Cache>>;

#[tokio::main]
async fn main() {
    let cache = Arc::new(Mutex::new(Cache::new(300)));

    let args: Vec<String> = std::env::args().collect();

    if args.len() < 2 {
        eprintln!("Usage: cargo run <API_KEY>");
        return;
    }

    let api_key = &args[1];

    rocket::build()
        .manage(cache)
        .manage(api_key.clone())
        .mount("/", routes![handle_connection])
        .launch()
        .await
        .unwrap();
}

#[get("/<uuid>")]
async fn handle_connection(
    uuid: &str,
    api_key: &rocket::State<String>,
    cache: &rocket::State<SharedCache>,
) -> Json<Value> {
    let start_time = Utc::now();

    {
        let mut cache_lock = cache.lock().unwrap();

        if let Some(cached_json) = cache_lock.get(uuid) {
            println!("Using cached data!");
            let duration = Utc::now().signed_duration_since(start_time);
            println!("Response time for UUID {}: {} seconds", uuid, duration.num_milliseconds() as f64 / 1000.0);
            return Json(cached_json);
        }
    }

    let client = reqwest::Client::new();
    let mut headers = HeaderMap::new();
    headers.insert(CONTENT_TYPE, HeaderValue::from_str("application/json").unwrap());
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
                println!("Response time for UUID {}: {} seconds", uuid, duration.num_milliseconds() as f64 / 1000.0);

                Json(formatted_json)
            } else {
                let duration = Utc::now().signed_duration_since(start_time);
                println!("Failed Response time for UUID {}: {} seconds", uuid, duration.num_milliseconds() as f64 / 1000.0);

                Json(serde_json::json!({
                    "error": format!("Request failed with status: {}", response.status())
                }))
            }

        }
        Err(e) => {
            let duration = Utc::now().signed_duration_since(start_time);
            println!("Failed Response time for UUID {}: {} seconds", uuid, duration.num_milliseconds() as f64 / 1000.0);

            Json(serde_json::json!({
                "error": format!("Request failed with error: {}", e)
            }))
        }
    }
}

mod cache;
mod format;

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use rocket::{get, routes, serde::json::Json};
use reqwest::header::{HeaderMap, HeaderValue, CONTENT_TYPE};
use serde_json::Value;
use lz4::{EncoderBuilder, Decoder};
use std::io::{Read, Write};
use lru::LruCache;
use chrono::{DateTime, Utc};

use cache::{Cache};
use format::format_numbers;

type SharedCache = Arc<Mutex<Cache>>;

// Rocket's main function
#[tokio::main]
async fn main() {
    let cache = Arc::new(Mutex::new(Cache::new(100))); // Create a cache with a capacity of 100 entries

    let args: Vec<String> = std::env::args().collect();

    // Check if API key is provided as a terminal argument
    if args.len() < 2 {
        eprintln!("Usage: cargo run <API_KEY>");
        return;
    }

    let api_key = &args[1];

    rocket::build()
        .manage(cache) // Pass the cache to the Rocket state
        .manage(api_key.clone())
        .mount("/", routes![handle_connection])
        .launch()
        .await
        .unwrap();
}

// Route handler
#[get("/<uuid>")]
async fn handle_connection(
    uuid: &str,
    api_key: &rocket::State<String>,
    cache: &rocket::State<SharedCache>,
) -> Json<Value> {
    let start_time = Utc::now();

    // Lock the cache only for the duration of this operation
    {
        let mut cache_lock = cache.lock().unwrap();

        // Check if the UUID is already in the cache
        if let Some(cached_json) = cache_lock.get(uuid) {
            println!("Using cached data!");
            let duration = Utc::now().signed_duration_since(start_time);
            println!("Response time for UUID {}: {} seconds", uuid, duration.num_milliseconds() as f64 / 1000.0);
            return Json(cached_json); // Return cached data
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
                println!("Raw response: {}", response_text);
                let json_body = serde_json::from_str::<Value>(&response_text).unwrap();

                let formatted_json = format_numbers(&json_body);

                // Insert into cache
                {
                    let mut cache_lock = cache.lock().unwrap();
                    cache_lock.insert(uuid.to_string(), &formatted_json);
                } // Release the lock after inserting into cache

                let duration = Utc::now().signed_duration_since(start_time);
                println!("Response time for UUID {}: {} seconds", uuid, duration.num_milliseconds() as f64 / 1000.0);

                Json(formatted_json)
            } else {
                let duration = Utc::now().signed_duration_since(start_time);
                println!("Response time for UUID {}: {} seconds", uuid, duration.num_milliseconds() as f64 / 1000.0);

                Json(serde_json::json!({
                    "error": format!("Request failed with status: {}", response.status())
                }))
            }

        }
        Err(e) => {
            let duration = Utc::now().signed_duration_since(start_time);
            println!("Response time for UUID {}: {} seconds", uuid, duration.num_milliseconds() as f64 / 1000.0);

            Json(serde_json::json!({
                "error": format!("Request failed with error: {}", e)
            }))
        }
    }
}

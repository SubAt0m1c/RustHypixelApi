use crate::cache_enum::CacheEnum;
use actix_web::http::StatusCode;
use actix_web::{web, HttpResponse, Responder};
use reqwest::Client;
use serde_json::Value;
use std::sync::Arc;
use std::time::Duration;

pub async fn fetch_and_cache(
    url: String,
    cache_key: String,
    client: web::Data<Arc<Client>>,
    cache: web::Data<CacheEnum>,
    duration: Duration,
    format: impl FnOnce(&Value) -> Value,
) -> actix_web::Result<impl Responder> {
    if let Some(cached) = cache.get(&cache_key, duration).await {
        return Ok(HttpResponse::Ok().json(&cached));
    }
    
    match client.get(&url).send().await { 
        Ok(response) => {
            if response.status().is_success() {
                let formatted = format(&response.json().await.unwrap());
                cache.insert(cache_key, &formatted, duration).await;
                Ok(HttpResponse::Ok().json(&formatted))
            } else {
                Ok(HttpResponse::new(StatusCode::from_u16(response.status().as_u16()).unwrap()))
            }
        }
        Err(e) => {
            println!("Error: {}", e);
            Ok(HttpResponse::new(StatusCode::INTERNAL_SERVER_ERROR))
        }
    }
}
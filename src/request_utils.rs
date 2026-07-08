use std::sync::LazyLock;

use actix_web::http::header::ContentType;
use actix_web::web::Bytes;
use actix_web::{mime, HttpResponse};
use reqwest::{header:: HeaderMap, Client};
use tokio::time::Instant;

use crate::API_KEY;
use crate::cache::UuidKey;
use crate::error::ProcessError;
use crate::logging::{LogMessage, log};

static CLIENT: LazyLock<Client> = LazyLock::new(|| {
    let api_key = API_KEY.get().expect("Api key should have been set already!");

    let mut headers = HeaderMap::new();
    headers.insert("API-Key", api_key.parse().unwrap());
    
    Client::builder()
        .default_headers(headers.clone())
        .build()
        .unwrap()
});

pub async fn request(key: UuidKey, url: String) -> Result<Bytes, ProcessError> {
    let now = Instant::now();
    let res = CLIENT.get(url).send().await?;
    log(LogMessage::ElapsedUserStatus { key, elapsed: now.elapsed(), message: "Upstream hit", code: res.status().as_u16() });
    res.error_for_status()?.bytes().await.map_err(ProcessError::from)
}

pub fn json_response(data: Bytes) -> HttpResponse {
    HttpResponse::Ok()
        .append_header(ContentType(mime::APPLICATION_JSON))
        .body(data)
}
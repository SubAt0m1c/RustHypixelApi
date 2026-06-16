use std::sync::LazyLock;

use actix_web::http::header::ContentType;
use actix_web::web::Bytes;
use actix_web::{mime, HttpResponse};
use reqwest::{header:: HeaderMap, Client};

use crate::API_KEY;
use crate::error::ProcessError;

static CLIENT: LazyLock<Client> = LazyLock::new(|| {
    let api_key = API_KEY.get().expect("Api key should have been set already!");

    let mut headers = HeaderMap::new();
    headers.insert("API-Key", api_key.parse().unwrap());
    
    Client::builder()
        .default_headers(headers.clone())
        .build()
        .unwrap()
});

pub async fn request(url: String) -> Result<Bytes, ProcessError> {
    let res = CLIENT.get(&url).send().await?;
    res.error_for_status()?.bytes().await.map_err(ProcessError::from)
}

pub fn json_response(data: Bytes) -> HttpResponse {
    HttpResponse::Ok()
        .append_header(ContentType(mime::APPLICATION_JSON))
        .body(data)
}
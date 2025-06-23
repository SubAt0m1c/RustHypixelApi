use actix_web::web::Bytes;
use actix_web::{web, HttpResponse};
use reqwest::Client;
use std::sync::Arc;

pub fn json_response(data: Bytes) -> HttpResponse {
    HttpResponse::Ok()
        .append_header(("Content-Type", "application/json"))
        .body(data)
}

pub async fn fetch(url: String, client: web::Data<Arc<Client>>) -> Result<Bytes, reqwest::Error> {
    let res = client.get(&url).send().await?;
    Ok(res.error_for_status()?.bytes().await?)
}

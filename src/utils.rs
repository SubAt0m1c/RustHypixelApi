use actix_web::http::header::ContentType;
use actix_web::web::{Bytes, Data};
use actix_web::{mime, HttpResponse};
use reqwest::Client;

pub fn json_response(data: Bytes) -> HttpResponse {
    HttpResponse::Ok()
        .append_header(ContentType(mime::APPLICATION_JSON))
        .body(data)
}

pub async fn fetch(url: String, client: &Client) -> Result<Bytes, reqwest::Error> {
    let res = client.get(&url).send().await?;
    Ok(res.error_for_status()?.bytes().await?)
}

pub async fn fetch_owned(url: String, client: Data<Client>) -> Result<Bytes, reqwest::Error> {
    let res = client.get(&url).send().await?;
    Ok(res.error_for_status()?.bytes().await?)
}

pub async fn get(url: String, client: &Client, body: Bytes) -> Result<Bytes, reqwest::Error> {
    let res = client.get(&url).body(body).send().await?;
    Ok(res.error_for_status()?.bytes().await?)
}

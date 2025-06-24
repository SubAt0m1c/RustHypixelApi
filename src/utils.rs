use actix_web::http::header::ContentType;
use actix_web::web::Bytes;
use actix_web::{mime, web, HttpResponse};
use reqwest::Client;

pub fn json_response(data: Bytes) -> HttpResponse {
    HttpResponse::Ok()
        .append_header(ContentType(mime::APPLICATION_JSON))
        .body(data)
}

pub async fn fetch(url: String, client: web::Data<Client>) -> Result<Bytes, reqwest::Error> {
    let res = client.get(&url).send().await?;
    Ok(res.error_for_status()?.bytes().await?)
}

use actix_web::http::header::ContentType;
use actix_web::web::Bytes;
use actix_web::{mime, HttpResponse};

pub fn json_response(data: Bytes) -> HttpResponse {
    HttpResponse::Ok()
        .append_header(ContentType(mime::APPLICATION_JSON))
        .body(data)
}
use std::{error::Error, fmt::Display};

use actix_web::{ResponseError, http::StatusCode};
use reqwest::Error as ReqwestError;
use simd_json::Error as SimdError;
use serde_json::Error as SerdeError;


#[derive(Clone, Debug)]
pub enum ProcessError {
    InternalServerError(&'static str),
    RequestError(u16),
    SerializationError(String),
}

impl ProcessError {
    pub const fn internal(msg: &'static str) -> Self {
        Self::InternalServerError(msg)
    }
}

impl Display for ProcessError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl Error for ProcessError {}

impl From<ReqwestError> for ProcessError {
    fn from(value: ReqwestError) -> Self {
        Self::RequestError(value.status().map(|s| s.as_u16()).unwrap_or(500))
    }
}

impl From<SimdError> for ProcessError {
    fn from(value: SimdError) -> Self {
        Self::SerializationError(value.to_string())
    }
}

impl From<SerdeError> for ProcessError {
    fn from(value: SerdeError) -> Self {
        Self::SerializationError(value.to_string())
    }
}

impl ResponseError for ProcessError {
    fn status_code(&self) -> actix_web::http::StatusCode {
        match self {
            Self::RequestError(code) => StatusCode::from_u16(*code).expect("Status code should never be invalid!"),
            _ => StatusCode::INTERNAL_SERVER_ERROR,
        }
    }
}
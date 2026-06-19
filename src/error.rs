use std::{error::Error, fmt::Display};

use actix_web::{ResponseError, http::StatusCode as ActixStatusCode};
use reqwest::{Error as ReqwestError, StatusCode};
use simd_json::Error as SimdError;
use serde_json::Error as SerdeError;


#[derive(Clone, Debug)]
pub enum ProcessError {
    InternalServerError(&'static str),
    RequestError(StatusCode),
    SerializationError(String),
}

impl ProcessError {
    pub const fn internal(msg: &'static str) -> Self {
        Self::InternalServerError(msg)
    }
}

impl Display for ProcessError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InternalServerError(msg) => write!(f, "{}: {}", StatusCode::INTERNAL_SERVER_ERROR, msg),
            Self::RequestError(error_code) => error_code.fmt(f),
            Self::SerializationError(msg) => write!(f, "{}: {}", StatusCode::INTERNAL_SERVER_ERROR, msg),
        }
    }
}

impl Error for ProcessError {}

impl From<ReqwestError> for ProcessError {
    fn from(value: ReqwestError) -> Self {
        Self::RequestError(value.status().unwrap_or(StatusCode::INTERNAL_SERVER_ERROR))
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
    fn status_code(&self) -> ActixStatusCode {
        match self {
            Self::RequestError(code) => ActixStatusCode::from_u16(code.as_u16()).unwrap_or(ActixStatusCode::INTERNAL_SERVER_ERROR),
            _ => ActixStatusCode::INTERNAL_SERVER_ERROR,
        }
    }
}
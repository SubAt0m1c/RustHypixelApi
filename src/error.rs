use std::{error::Error, fmt::Display};

use actix_web::{ResponseError, http::StatusCode as ActixStatusCode};
use reqwest::{Error as ReqwestError, StatusCode};
use simd_json::Error as SimdError;
use serde_json::Error as SerdeError;


#[derive(Clone, Debug)]
pub enum ProcessError {
    InternalServer(&'static str),
    Request(StatusCode),
    Serialization(String),
    Database(String),
}

impl ProcessError {
    pub const fn internal(msg: &'static str) -> Self {
        Self::InternalServer(msg)
    }
}

impl Display for ProcessError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InternalServer(msg) => write!(f, "{}: {}", StatusCode::INTERNAL_SERVER_ERROR, msg),
            Self::Request(error_code) => write!(f, "{error_code}: Request Error"),
            Self::Serialization(msg) | 
            Self::Database(msg) => write!(f, "{}: {}", StatusCode::INTERNAL_SERVER_ERROR, msg),
        }
    }
}

impl Error for ProcessError {}

impl From<ReqwestError> for ProcessError {
    fn from(value: ReqwestError) -> Self {
        Self::Request(value.status().unwrap_or(StatusCode::INTERNAL_SERVER_ERROR))
    }
}

impl From<SimdError> for ProcessError {
    fn from(value: SimdError) -> Self {
        Self::Serialization(value.to_string())
    }
}

impl From<SerdeError> for ProcessError {
    fn from(value: SerdeError) -> Self {
        Self::Serialization(value.to_string())
    }
}

impl From<ltmdb::Error> for ProcessError {
    fn from(value: ltmdb::Error) -> Self {
        Self::Database(value.to_string())
    }
}

impl ResponseError for ProcessError {
    fn status_code(&self) -> ActixStatusCode {
        match self {
            Self::Request(code) => ActixStatusCode::from_u16(code.as_u16()).unwrap_or(ActixStatusCode::INTERNAL_SERVER_ERROR),
            _ => ActixStatusCode::INTERNAL_SERVER_ERROR,
        }
    }
}
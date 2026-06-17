use std::{fmt::Display, thread, time::Duration};

use actix_web::cookie::time::UtcDateTime;
use tokio::sync::{OnceCell, mpsc::{UnboundedSender, unbounded_channel}};

use crate::cache::cache_key::CacheKey;

pub enum LogMessage {
    TimeElapsed {
        elapsed: Duration,
        name: &'static str,
    },
    ElapsedAndUser {
        key: CacheKey,
        elapsed: Duration,
        message: &'static str,
    },
    MessageAndUser {
        key: CacheKey,
        message: &'static str,
    },
}

impl Display for LogMessage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::TimeElapsed { elapsed, name} => {
                write!(f, "Time elapsed for {}: {:?}", name, elapsed)
            }
            Self::ElapsedAndUser { key, elapsed, message } => {
                write!(f, "{}: {:?} ({:?})", message, key, elapsed)
            }
            Self::MessageAndUser { key, message: field } => {
                write!(f, "{}: {:?}", field, key)
            }
        }
    } 
}

static SENDER: OnceCell<UnboundedSender<LogMessage>> = OnceCell::const_new();

pub fn log(msg: LogMessage) {
    if let Some(tx) = SENDER.get() {
        let _ = tx.send(msg);
    }
}

pub fn init() {
    let (tx, mut rx) = unbounded_channel::<LogMessage>();
    SENDER.set(tx).unwrap();
    thread::spawn(move || {
        while let Some(message) = rx.blocking_recv() {
            println!("{}: {}", UtcDateTime::now(), message)
        }
    });
}
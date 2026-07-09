use std::{fmt::Display, thread, time::Duration};

use actix_web::cookie::time::UtcDateTime;
use tokio::sync::{OnceCell, mpsc::{UnboundedSender, unbounded_channel}};

use crate::cache::UuidKey;

pub enum LogMessage {
    TimeElapsed {
        elapsed: Duration,
        name: &'static str,
    },
    // ElapsedAndUser {
    //     key: CacheKey,
    //     elapsed: Duration,
    //     message: &'static str,
    // },
    ElapsedUserStatus {
        key: UuidKey,
        elapsed: Duration,
        message: &'static str,
        code: u16,
    },
    MessageAndUser {
        key: UuidKey,
        message: &'static str,
    },
}

impl Display for LogMessage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::TimeElapsed { elapsed, name} => {
                write!(f, "Time elapsed for {name}: {elapsed:?}")
            }
            // Self::ElapsedAndUser { key, elapsed, message } => {
            //     write!(f, "{}: {:?} ({:?})", message, key, elapsed)
            // }
            Self::ElapsedUserStatus { key, elapsed, message, code } => {
                write!(f, "{message}: {key} ({code}; {elapsed:?})")
            }
            Self::MessageAndUser { key, message: field } => {
                write!(f, "{field}: {key}")
            }
        }
    } 
}

static SENDER: OnceCell<UnboundedSender<(UtcDateTime, LogMessage)>> = OnceCell::const_new();

pub fn log(msg: LogMessage) {
    let _ = SENDER.get().expect("Logger should be initialized").send((UtcDateTime::now(), msg));
}

pub fn init() {
    let (tx, mut rx) = unbounded_channel::<(UtcDateTime, LogMessage)>();
    SENDER.set(tx).unwrap();
    thread::spawn(move || {
        while let Some((time, msg)) = rx.blocking_recv() {
            println!("{time}: {msg}");
        }
    });
}
use std::{fmt::Display, thread, time::Duration};

use actix_web::cookie::time::UtcDateTime;
use tokio::sync::{OnceCell, mpsc::{UnboundedSender, unbounded_channel}};
use uuid::Uuid;

pub enum LogMessage {
    TimeElapsed {
        elapsed: Duration,
        name: &'static str,
    },
    MessageAndUser {
        id: Uuid,
        message: &'static str,
    },
    AwaitingSameRequest {
        id: Uuid,
    }
}

impl Display for LogMessage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::TimeElapsed { elapsed, name} => {
                write!(f, "Time elapsed for {}: {:?}", name, elapsed)
            }
            Self::MessageAndUser { id, message: field } => {
                write!(f, "{}: {}", field, id)
            }
            Self::AwaitingSameRequest { id } => {
                write!(f, "Requested same user as already being requested, id: {}", id)
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
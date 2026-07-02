//! ltmdb is a lifetime managed database.
//! entries are mapped by their ttl (time to live) to a file.
//! files are written to during a window, after which a new
//! file begins being written to. After the time to live of
//! the file has passed, it is entirely deleted, removing all
//! entries from that file.
//! 
//! Due to this, entries should not expect their lifetime
//! to match their ttl exactly, but rather be a "good enough"
//! approximation. 
//! 
//! As async as possible, with file io being run by the db
//! runtime.

use std::{result::Result as StdResult, time::{Duration, SystemTime, UNIX_EPOCH}};

mod error;
mod expiration_queue;
mod file_handle;
mod partition;
mod bucket;
mod db;
mod sized_bytes;
mod runtime;

pub use error::{ Error, ErrorKind, ResultExt };
pub use db::Database;
pub use runtime::Runtime;

pub(crate) type Result<T> = StdResult<T, error::Error>;

#[inline]
pub(crate) fn unix_secs() -> u64 {
   SystemTime::now()
       .duration_since(UNIX_EPOCH)
       .unwrap_or(Duration::ZERO)
       .as_secs()
}
//! ltmdb is a lifetime managed key-value store.
//! entries are mapped by their ttl (time to live) to a file.
//! 
//! This database does not function by transactions.
//! Writes are given to the os immedietely. 
//! There are no durability gurantees.
//! Reads after writes are guranteed to include the data written by the write.
//! Concurrent reads do not support this gurantee.
//! 
//! Due to file-batched removals, entries should not expect their lifetime
//! to match their ttl exactly, but rather be a "good enough"
//! approximation. 

use std::{result::Result as StdResult, time::{Duration, SystemTime, UNIX_EPOCH}};

mod error;
mod expiration_queue;
mod file_handle;
mod partition;
mod bucket;
mod db;
mod sized_bytes;
mod runtime;

pub use error::{Error, ErrorKind, ResultExt};
pub use db::Database;
pub use runtime::Runtime;
pub use sized_bytes::SizedBytes;

pub(crate) type Result<T> = StdResult<T, error::Error>;
#[inline]
pub(crate) fn unix_secs() -> u64 {
   SystemTime::now()
       .duration_since(UNIX_EPOCH)
       .unwrap_or(Duration::ZERO)
       .as_secs()
}
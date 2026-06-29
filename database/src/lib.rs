use std::{result, time::{Duration, SystemTime, UNIX_EPOCH}};


pub mod error;
mod expiration_queue;
mod file_handle;
mod partition;
mod bucket;
pub mod cache;
mod sized_bytes;
pub mod runtime;

pub type Result<T> = result::Result<T, error::Error>;

#[inline]
pub(crate) fn unix_secs() -> u64 {
   SystemTime::now()
       .duration_since(UNIX_EPOCH)
       .unwrap_or(Duration::ZERO)
       .as_secs()
}

// #[cfg(test)]
// mod tests {
//     use bytes::Bytes;

//     use crate::{cache::Cache, runtime::StdThread};

//     use super::*;

//     #[test]
//     fn it_works() {
//         futures::executor::block_on(async {
//             let test = Cache::new(StdThread);
//             test.insert(129, Bytes::new(), Duration::from_secs(120)).await.unwrap();
//             let res = test.read(129).await.unwrap();
//         });
//     }
// }

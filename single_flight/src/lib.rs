//! Asyncronous concurrent single flight request deduplication.
//! Runtime agnostic, built for high concurrency.
//! 
//! Heavily based on [async_singleflight](https://github.com/PureWhiteWu/async_singleflight) under the MIT license.
//! Other crates did not offer the mostly lock-free design I was looking for.

mod types;
mod group;
mod error;

pub use group::*;
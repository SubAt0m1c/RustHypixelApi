//! ltmdb is a lifetime managed key-value store.
//! entries are mapped by their ttl (time to live) to a file.
//! files are written to during a window, after which a new
//! file begins being written to. After the ttl of
//! the file has passed, it is entirely deleted, removing all
//! entries from that file.
//! 
//! Due to file-batched removals, entries should not expect their lifetime
//! to match their ttl exactly, but rather be a "good enough"
//! approximation. 

#![allow(clippy::cast_possible_truncation, clippy::cast_possible_wrap)]

use std::{result::Result as StdResult, time::{Duration, SystemTime, UNIX_EPOCH}};

mod hasher;
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

pub(crate) mod defer {
    use std::mem::ManuallyDrop;

    /// runs the given closure when the returned value is dropped.
    pub fn defer<F: FnOnce()>(deferred: F) -> impl Deferred {
        struct Deferrable<F: FnOnce()>(ManuallyDrop<F>);

        impl<F: FnOnce()> Sealed for Deferrable<F> {}
        
        impl<F: FnOnce()> Deferred for Deferrable<F> {
            /// consumes the deferred closue without running it.
            fn cancel(self) {
                let mut guard = ManuallyDrop::new(self);
    
                // SAFETY: `guard` is wrapped in an outer [`ManuallyDrop`], so it's own
                // destructor will never be run. This is the only place `f` is dropped.
                unsafe { ManuallyDrop::drop(&mut guard.0) };
            }
        }
        
        impl<F: FnOnce()> Drop for Deferrable<F> {
            fn drop(&mut self) {
                // SAFETY: We don't use the internal [`ManuallyDrop`] after this.
                let f = unsafe { ManuallyDrop::take(&mut self.0) };
                f();
            }
        }
        
        Deferrable(ManuallyDrop::new(deferred))
    }

    #[allow(private_bounds)]
    pub trait Deferred: Sealed {
        fn cancel(self);
    }
    
    trait Sealed {}
}
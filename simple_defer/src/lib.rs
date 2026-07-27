use std::mem::ManuallyDrop;

/// runs the given closure when the returned value is dropped.
#[must_use]
pub fn defer<R, F: FnOnce() -> R>(deferred: F) -> impl Deferred {
    struct Deferrable<R, F: FnOnce() -> R>(ManuallyDrop<F>);

    impl<R, F: FnOnce() -> R> Sealed for Deferrable<R, F> {}
    
    impl<R, F: FnOnce() -> R> Deferred for Deferrable<R, F> {
        /// consumes the deferred closue without running it.
        fn cancel(self) {
            let mut guard = ManuallyDrop::new(self);

            // SAFETY: `guard` is wrapped in an outer [`ManuallyDrop`], so it's own
            // destructor will never be run. This is the only place `f` is dropped.
            unsafe { ManuallyDrop::drop(&mut guard.0) };
        }
    }
    
    impl<R, F: FnOnce() -> R> Drop for Deferrable<R, F> {
        fn drop(&mut self) {
            // SAFETY: We don't use the internal [`ManuallyDrop`] after this.
            let f = unsafe { ManuallyDrop::take(&mut self.0) };
            let _ = f();
        }
    }
    
    Deferrable(ManuallyDrop::new(deferred))
}

#[allow(private_bounds)]
pub trait Deferred: Sealed {
    fn cancel(self);
}

trait Sealed {}
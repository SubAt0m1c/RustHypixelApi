//! A concurrent lock-free cell.
//! 
//! Similar in idea to [ArcSwap](https://crates.io/crates/arc-swap) but doesn't require the inner value to be arced.
//! This is accomplished by using [seize](https://crates.io/crates/seize) memory reclaimation.
//! Use `ArcSwap` if the internal value will be arced anways.
//! 
//! Exposes an api similar to that of [papaya](https://crates.io/crates/papaya)

use std::{marker::PhantomData, mem::forget, ptr, sync::{Arc, atomic::{AtomicPtr, Ordering}}};

use seize::{Guard, reclaim};

pub use seize::Collector as Collector;
pub use seize::LocalGuard as LocalGuard;
pub use seize::OwnedGuard as OwnedGuard;

/// A concurrent, lock-free cell that holds a value.
pub struct ConcurrentCell<T> {
    collector: Arc<Collector>,
    value: AtomicPtr<T>,
    _marker: PhantomData<T>, // ensures the cell is only `Send` and `Sync` if `T` is `Send` and `Sync`.
}

impl<T> ConcurrentCell<T> {
    /// Creates a new `ConcurrentCell` initialized with the given value.
    pub fn new(value: T) -> Self {
        Self::with_collector(value, Arc::new(Collector::new()))
    }

    /// Creates a new `ConcurrentCell` with the given value and collector.
    /// 
    /// Enables using one collector with multiple concurrent cells.
    pub fn with_collector(value: T, collector: Arc<Collector>) -> Self {
        let ptr = Box::into_raw(Box::new(value));
        Self {
            collector,
            value: AtomicPtr::new(ptr),
            _marker: PhantomData
        }
    }

    /// Returns a guard for this cell.
    /// 
    /// Note that holding a guard prevents garbage collection.
    pub fn guard(&self) -> LocalGuard<'_> {
        self.collector.enter()
    }

    /// Returns an owned guard for this cell.
    /// This guard is [`Send`] and [`Sync`].
    /// 
    /// Note that holding a guard prevents garbage collection.
    pub fn guard_owned(&self) -> OwnedGuard<'_> {
        self.collector.enter_owned()
    }

    /// Pins this cell, enabling a more frendly, user guard-free way to access values.
    /// 
    /// Internally holds a guard to itself, so it prevents garbage collection.
    pub fn pin(&self) -> PinnedCell<'_, T, LocalGuard<'_>> {
        PinnedCell {
            guard: self.guard(),
            cell: self,
        }
    }

    /// Pins this cell, enabling a more frendly, user guard-free way to access values.
    /// This pinned cell reference is [`Send`] and [`Sync`].
    /// 
    /// Internally holds an owned guard to itself, so it prevents garbage collection.
    pub fn pin_owned(&self) -> PinnedCell<'_, T, OwnedGuard<'_>> {
        PinnedCell {
            guard: self.guard_owned(),
            cell: self,
        }
    }
    
    /// Gets a reference to the value stored in this cell.
    /// 
    /// Values will see a 'snapshot' of the value at the time of its load.
    /// Concurrent writes will not be visible until `get` is called again.
    pub fn get<'g, G: Guard>(&self, guard: &'g G) -> &'g T {
        let ptr = guard.protect(&self.value, Ordering::Acquire);
        // SAFETY: We have protected the value, so the pointer will not be freed for as long as the guard is alive.
        unsafe { &*ptr }
    }

    /// Sets the value to the given value atomically.
    /// 
    /// Existing readers will not see the new value until they call `get` or `get_owned` again.
    pub fn set(&self, new: T) {
        let new_ptr = Box::into_raw(Box::new(new));
        let old = self.value.swap(new_ptr, Ordering::AcqRel);

        // SAFETY: The above swap ensures that no new thread may access the old value.
        unsafe { self.collector.retire(old, reclaim::boxed); }
    }

    /// Atomically updates the internal value using the given closure.
    /// 
    /// The closure is given the current state of the cell and is set to the new value.
    pub fn update(&self, f: impl Fn(&T) -> T) {
        self.pin().compute(|t| Operation::Set(f(t)));
    }

    /// Atomically updates the internal value using the given closure.
    /// 
    /// The closure is given the current state of the cell and updates the value according to `Update`.
    /// This function closure should be pure as it may be retried in the event of concurrent modifications.
    /// 
    /// Returns a `Compute` enum that can be used to inspect the result of the update.
    /// This enables implementing complex functions atomically such as `get_or_set` or `set_if`.
    pub fn compute<'g, G: Guard>(&self, f: impl Fn(&T) -> Operation<T>, guard: &'g G) -> Compute<'g, T> {
        // Box and pointer gymnastics so we only allocate once instead of per retry.
        let mut new_box = Box::<T>::new_uninit();
        let new_ptr = new_box.as_mut_ptr();
        
        loop {
            let current = guard.protect(&self.value, Ordering::Acquire);
            // SAFETY: We have protected the value, so the pointer will not be freed for as long as the guard is alive.
            let result = f(unsafe { &*current});
            let Operation::Set(new) = result else { return Compute::Aborted };

            // SAFETY: The pointer was initialized by a Box and can be safely written to.
            unsafe { new_ptr.write(new); }

            // weak because were already in a retry loop
            match guard.compare_exchange_weak(&self.value, current, new_ptr, Ordering::Release, Ordering::Acquire) {
                Ok(current) => {
                    // leak the boxed pointer into the AtomicPtr so the collector can free it later.
                    forget(new_box);
                    
                    // SAFETY: We currently have a guard active so this pointer cannot be freed until its dropped.
                    let new_value = unsafe { &*new_ptr };
                    // SAFETY: guard.compare_exchange gurantees that this pointer is safe is if it were protected by guard.protect().
                    let old_value = unsafe { &*current };

                    // SAFETY: We have swapped out the old pointer so no new threads may access it.
                    unsafe { guard.defer_retire(current, reclaim::boxed); } // defer the retire because we want to keep the old value alive to return it.
                    
                    return Compute::Set { old: old_value, new: new_value }
                },
                // SAFETY: new_ptr has been initialized and can be safely dropped in place for box reuse.
                Err(_) => unsafe { ptr::drop_in_place(new_box.as_mut_ptr()); }
            }
        }
    }
}

impl<T> Drop for ConcurrentCell<T> {
    fn drop(&mut self) {
        let ptr = *self.value.get_mut();
        // SAFETY: The last value in the cell hasn't yet been retired, so it is safe to drop.
        unsafe { drop(Box::from_raw(ptr)); }
    }
}

/// `PinnedCell` acts as a reference to a concurrent cell that owns a `guard`.
pub struct PinnedCell<'a, T, G: Guard> {
    guard: G,
    cell: &'a ConcurrentCell<T>,
}

impl<T, G: Guard> PinnedCell<'_, T, G> {
    /// Gets a reference to the value stored in this cell.
    pub fn get(&self) -> &T {
        self.cell.get(&self.guard)
    }

    /// Sets the value to the given value atomically.
    /// 
    /// Existing readers will not see the new value until they call `get` or `get_owned` again.
    pub fn set(&mut self, value: T) {
        self.cell.set(value);
    }

    /// Atomically updates the internal value using the given closure.
    /// 
    /// The closure is given the current state of the cell and updates the value according to `Update`.
    /// This function should be pure as it may be retried in the event of concurrent modifications.
    /// 
    /// Returns a `Compute` enum that can be used to inspect the result of the update.
    /// This enables implementing complex functions such as `get_or_set` or `set_if`.
    pub fn compute(&self, f: impl Fn(&T) -> Operation<T>) -> Compute<'_, T> {
        self.cell.compute(f, &self.guard)
    }
}

/// Represents the result of an update operation.
/// 
/// `Set` will set the value to `T`, while `Abort` will not update the value.
pub enum Operation<T> {
    /// Indicates that the value should be updated to `T`.
    Set(T),
    /// Indicates that the value will not be updated.
    Abort,
}

/// Represents the result of a compute operation.
pub enum Compute<'a, T> {
    Set {
        old: &'a T,
        new: &'a T,
    },
    Aborted
}
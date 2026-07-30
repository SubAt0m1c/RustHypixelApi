//! A simple, lockable arc. Based on the moka [`MiniArc`](https://github.com/moka-rs/moka/blob/e617b5f064cdb3ce9845cef06961fdbf07bd9946/src/common/concurrent/arc.rs)
//! 
//! Really specific use case for us but it solves a problem!

#![allow(unused)]

use std::{ops::Deref, ptr::NonNull, sync::atomic::{self, AtomicU64, Ordering}};

const NONE_AND_OPEN: u64 = 0;
const NO_OTHERS_AND_OPEN: u64 = 1;
const LOCKED: u64 = 1 << 63;
const LOCKED_ONE: u64 = LOCKED | NO_OTHERS_AND_OPEN;

const MAX_REFCOUNT: u64 = u64::MAX >> 1;

/// An (un)locked atomically reference counted pointer.
/// 
/// This pointer will hold the door open and unlocked, unless its forced closed.
pub struct Larc<T: ?Sized> {
    ptr: NonNull<LarcInner<T>>,
}

impl<T: ?Sized> Pointer for Larc<T> {
    type Pointee = LarcInner<T>;

    fn ptr(this: &Self) -> NonNull<Self::Pointee> {
        this.ptr
    }
}

unsafe impl<T: ?Sized + Send + Sync> Send for Larc<T> {}
unsafe impl<T: ?Sized + Send + Sync> Sync for Larc<T> {}
unsafe impl<T: ?Sized + Send + Sync> Send for Darc<T> {}
unsafe impl<T: ?Sized + Send + Sync> Sync for Darc<T> {}

/// A door to an atomically reference counted pointer.
/// 
/// This pointer must be opened to access `T`.
pub struct Darc<T: ?Sized> {
    ptr: NonNull<LarcInner<T>>,
}

impl<T: ?Sized> Pointer for Darc<T> {
    type Pointee = LarcInner<T>;

    fn ptr(this: &Self) -> NonNull<Self::Pointee> {
        this.ptr
    }
}

impl<T> Darc<T> {
    pub fn new(data: T) -> Self {
        let ptr = Box::into_raw(Box::new(LarcInner {
            accessors: AtomicU64::new(1),
            open: AtomicU64::new(0),
            data,
        }));
        Self {
            ptr: NonNull::new(ptr).expect("Should have just allocated the ptr."),
        }
    }
}

impl<T: ?Sized> Darc<T> {
    /// Returns the number of accessors currently accessing this `Darc`.
    pub fn accessor_count(this: &Self, order: Ordering) -> u64 {
        this.data().accessors.load(order)
    }

    /// Returns the number of `Larc`s currently accessing this `Darc`.
    pub fn open_count(this: &Self, order: Ordering) -> u64 {
        this.data().open.load(order)
    }
    
    /// Checks if this `Darc` points to the same data as the other pointer.
    pub fn ptr_eq<P: Pointer<Pointee = LarcInner<T>>>(&self, other: &P) -> bool {
        std::ptr::eq(self.ptr.as_ptr(), Pointer::ptr(other).as_ptr())
    }

    fn data(&self) -> &LarcInner<T> {
        // SAFETY: This pointer will be valid as long as the Darc is alive.
        unsafe { self.ptr.as_ref() }
    }

    /// Checks if this `Darc` is locked.
    pub fn locked(&self) -> bool {
        self.data().open.load(Ordering::Acquire) & LOCKED != 0
    }

    /// Attempts to lock this `Darc` if there are no `Larc`s accessing it.
    /// 
    /// Returns `true` if we are locked with no `Larc`s accessing it, 
    /// `false` if there are any `Larc`s accessing it.
    pub fn try_lock(&self) -> bool {
        match self.data().open.compare_exchange(
            NONE_AND_OPEN,
            LOCKED,
            Ordering::AcqRel,
            Ordering::Acquire,
        ) {
            Ok(_) => true,
            Err(actual) => actual == LOCKED,
        }
    }

    /// Unconditionally locks this `Darc`.
    /// 
    /// No new `Larc`s can be created after this is called.
    pub fn lock(&self) {
        self.data().open.fetch_or(LOCKED, Ordering::AcqRel);
    }

    /// Attempts to open the `Darc` and provide owned access to the inner data.
    /// 
    /// Returns `None` if the `Darc` is locked.
    pub fn try_open(&self) -> Option<Larc<T>> {
        if self.data().try_inc_open() {
            self.data().accessors.fetch_add(1, Ordering::Relaxed);
            Some(Larc { ptr: self.ptr })
        } else {
            None
        }
    }
}

pub struct LarcInner<T: ?Sized> {
    accessors: AtomicU64,
    open: AtomicU64, // top bit: LOCKED
    data: T,
}

impl<T: ?Sized> LarcInner<T> {
    /// Attempts to increment the open count if the data is not locked.
    fn try_inc_open(&self) -> bool {
        let mut open_count = self.open.load(Ordering::Acquire);
        
        loop {
            if open_count & LOCKED != 0 { // locked
                return false;
            }

            if open_count == MAX_REFCOUNT { // we can't clone any more safely.
                return false;
            }

            match self.open.compare_exchange_weak(
                open_count, 
                open_count + 1, 
                Ordering::AcqRel, 
                Ordering::Acquire
            ) {
                Ok(_) => return true,
                Err(actual) => open_count = actual,
            }
        }
    }
}

impl<T: ?Sized> Larc<T> {
    /// Returns the number of accessors currently accessing the underlying `Darc`.
    pub fn accessor_count(this: &Self, order: Ordering) -> u64 {
        this.data().accessors.load(order)
    }

    /// Returns the number of other `Larc`s currently accessing the underlying `Darc`.
    pub fn open_count(this: &Self, order: Ordering) -> u64 {
        this.data().open.load(order)
    }

    /// Checks if this `Darc` points to the same data as the other pointer.
    pub fn ptr_eq<P: Pointer<Pointee = LarcInner<T>>>(this: &Self, other: &P) -> bool {
        std::ptr::eq(this.ptr.as_ptr(), Pointer::ptr(other).as_ptr())
    }

    /// Returns `true` if the backing `Darc` is locked, `false` otherwise.
    pub fn locked(this: &Self) -> bool {
        this.data().open.load(Ordering::Acquire) & LOCKED != 0
    }

    /// Attempts to lock the backing `Darc` if this is the last `Larc` accessing it.
    /// 
    /// Returns `true` if the `Darc` is locked and there is only one accessing `Larc`, 
    /// `false` if there are any other accessors.
    pub fn try_lock(this: &Self) -> bool {
        match this.data().open.compare_exchange(
            NO_OTHERS_AND_OPEN,
            LOCKED_ONE,
            Ordering::AcqRel,
            Ordering::Acquire,
        ) {
            Ok(_) => true,
            Err(actual) => actual == LOCKED_ONE
        }
    }

    /// Unconditionally locks the backing `Darc`.
    pub fn lock(this: &Self) {
        this.data().open.fetch_or(LOCKED, Ordering::AcqRel);
    }

    fn data(&self) -> &LarcInner<T> {
        // SAFETY: This pointer will be valid as long as the Larc is alive.
        unsafe { self.ptr.as_ref() }
    }
}

impl<T: ?Sized> Deref for Larc<T> {
    type Target = T;

    fn deref(&self) -> &T {
        &self.data().data
    }
}

impl<T: ?Sized> Drop for Darc<T> {
    fn drop(&mut self) {
        if self.data().accessors.fetch_sub(1, Ordering::Release) == 1 {
            atomic::fence(Ordering::Acquire);
            unsafe {
                drop(Box::from_raw(self.ptr.as_ptr()));
            }
        }
    }
}

impl<T: ?Sized> Drop for Larc<T> {
    fn drop(&mut self) {
        self.data().open.fetch_sub(1, Ordering::Release);
        
        if self.data().accessors.fetch_sub(1, Ordering::Release) == 1 {
            atomic::fence(Ordering::Acquire);
            unsafe { drop(Box::from_raw(self.ptr.as_ptr())); }
        }
    }
}

pub trait Pointer {
    type Pointee: ?Sized;
    
    fn ptr(this: &Self) -> NonNull<Self::Pointee>;
}
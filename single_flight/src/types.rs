use std::{pin::Pin, sync::{Arc, atomic::{AtomicU64, Ordering}}, task::{Context, Poll}};
use concurrent_cell::{Collector, Compute, ConcurrentCell, Operation, PinnedCell};
use conquer_util::BackOff;
use event_listener::{Event, EventListener};
use papaya::LocalGuard;
use pin_project_lite::pin_project;

pub(crate) enum FlightType<'a, T> {
    Follower(&'a Arc<Flight<T>>),
    Leader(&'a Arc<Flight<T>>),
}

pub(crate) enum ComputeEscape<T> {
    Follow,
    LeaderFailed,
    Success(T)
}

pub(crate) struct Flight<T> {
    follower_count: AtomicU64,
    result: ConcurrentCell<State<T>>,
    notify: BigNotify,
}

impl<T> Flight<T>  {
    pub fn with_collector(collector: Arc<Collector>) -> Self {
        Self {
            follower_count: AtomicU64::new(FollowCount::NONE_AND_OPEN),
            result: ConcurrentCell::with_collector(State::Uninit, collector),
            notify: BigNotify::new(),
        }
    }

    /// Attempts to close this flight if there are no followers.
    /// 
    /// returns `true` if it successfully closed the flight
    /// returns `false` if there are followers or it was already closed.
    pub fn close(self: &Arc<Self>) -> bool {
        self.follower_count.compare_exchange(
            FollowCount::NONE_AND_OPEN,
            FollowCount::CLOSING,
            Ordering::AcqRel,
            Ordering::Acquire,
        ).is_ok()
    }

    /// Attempts to acquire a following slot.
    /// Returns `false` if the flight is closed.
    /// returns `true` if it successfully incremented the follower counter.
    pub fn try_follow(self: &Arc<Self>) -> bool {
        let mut follow_count = FollowCount::load(&self.follower_count, Ordering::Acquire);
        let backoff = BackOff::random();
        
        loop {
            if follow_count.closing() {
                return false // flight is closing, we can't now start following; it will lead to us having a dangling flight.
            }

            match self.follower_count.compare_exchange_weak(
                follow_count.value(), 
                follow_count.value() + 1, 
                Ordering::AcqRel, 
                Ordering::Acquire
            ) {
                Ok(_) => return true, // we have successfully incremented the follower count so we know we can follow.
                Err(actual) => follow_count.set(actual),
            }

            // if external code leads to us getting slammed we want to ensure we handle as much throughput as possible.
            backoff.spin();
        }
    }

    pub fn drop_follower(&self) {
        self.follower_count.fetch_sub(1, Ordering::AcqRel);
    }

    pub fn set_state(&self, state: State<T>) {
        self.result.set(state);
        self.notify.notify_waiters();
    }
    
    pub fn update<V>(&self, f: impl Fn(&State<T>) -> Operation<State<T>, V>) {
        let pinned = self.cell_pinned();
        let res = pinned.compute(f);
        if matches!(res, Compute::Set { .. }) {
            // we only want to notify waiters if a new value was actually set.
            self.notify.notify_waiters();
        }
    }

    pub fn cell_pinned(&self) -> PinnedCell<'_, State<T>, LocalGuard<'_>> {
        self.result.pin()
    } 

    pub fn wait(&self) -> EventListener {
        self.notify.notified()
    }
}

pub(crate) struct FollowCount {
    inner: u64
}

impl FollowCount {
    const NONE_AND_OPEN: u64 = 0;
    const CLOSING: u64 = 1 << 63;
    
    pub fn load(value: &AtomicU64, order: Ordering) -> Self {
        Self {
            inner: value.load(order)
        }
    }

    pub fn set(&mut self, new: u64) {
        self.inner = new;
    }

    pub fn value(&self) -> u64 {
        self.inner
    }

    pub fn closing(&self) -> bool {
        self.inner & Self::CLOSING != 0
    }
}

pub(crate) enum State<T> {
    Uninit,
    Running,
    LeaderDropped,
    LeaderFailed,
    Success(T),
}

pin_project! {
    pub(crate) struct Leader<'a, T, F, Output>
    where
        T: Clone,
        F: Future<Output = Output>,
    {
        #[pin]
        fut: F,
        flight: &'a Arc<Flight<T>>,
    }
    
    impl<T, F, Output> PinnedDrop for Leader<'_, T, F, Output>
    where
        T: Clone,
        F: Future<Output = Output>,
    {
        fn drop(this: Pin<&mut Self>) {
            let this = this.project();
            this.flight.update(|s| {
                if matches!(s, State::Running) {
                    Operation::Set(State::LeaderDropped)
                } else {
                    Operation::Abort(())
                }
            });
        }
    }
}


impl<'a, T, F, Output> Leader<'a, T, F, Output>
where
    T: Clone,
    F: Future<Output = Output>,
{
    pub fn new(fut: F, flight: &'a Arc<Flight<T>>) -> Self {
        Self { fut, flight }
    }
}

impl<T, E, F> Future for Leader<'_, T, F, Result<T, E>>
where
    T: Clone,
    F: Future<Output = Result<T, E>>,
{
    type Output = Result<T, E>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.project();
        let result = this.fut.poll(cx);
        if let Poll::Ready(val) = &result {
            match val {
                Ok(v) => this.flight.set_state(State::Success(v.clone())),
                Err(_) => this.flight.set_state(State::LeaderFailed),
            }
        }
        result
    }
}

#[allow(clippy::mismatching_type_param_order)]
impl<T, F> Future for Leader<'_, T, F, T>
where
    T: Clone + Send + Sync,
    F: Future<Output = T>,
{
    type Output = T;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.project();
        let result = this.fut.poll(cx);
        if let Poll::Ready(val) = &result {
            this.flight.set_state(State::Success(val.clone()));
        }
        result
    }
}

/// Based off tokio's big notify in their watcher tasks.
/// Shards event notifiers/wakers to minimize internal lock contention.
pub(super) struct BigNotify {
    inner: [Event; 8]
}

impl BigNotify {
    pub(super) fn new() -> Self {
        Self {
            inner: Default::default(),
        }
    }
    
    pub(super) fn notify_waiters(&self) {
        for notify in &self.inner {
            notify.notify(usize::MAX);
        }
    }
    
    pub(super) fn notified(&self) -> EventListener {
        let i = fastrand::usize(..8);
        self.inner[i].listen()
    }
}
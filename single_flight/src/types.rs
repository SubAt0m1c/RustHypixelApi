use std::{borrow::Borrow, hash::{BuildHasher, Hash}, pin::Pin, sync::Arc, task::{Context, Poll}};
use concurrent_cell::{Collector, Compute, ConcurrentCell, Operation, PinnedCell};
use event_listener::{Event, EventListener};
use papaya::{HashMap, LocalGuard};
use pin_project_lite::pin_project;

use crate::larc::{Darc, Larc};

pub struct MapFlight<T> {
    flight: Darc<Flight<T>>,
}

impl<T> MapFlight<T> {
    pub fn new(flight: Darc<Flight<T>>) -> Self {
        Self { flight }
    }

    pub fn same_flight<Q, K, S>(&self, map_flight: &InFlight<'_, Q, K, T, S>) -> bool
    where 
        Q: Hash + Eq + ?Sized + Send + Sync + ToOwned<Owned = K>,
        K: Hash + Eq + Borrow<Q>,
        S: BuildHasher,
    {
        self.flight.ptr_eq(&map_flight.flight)
    }
}

pub(crate) struct InFlight<'a, Q, K, T, S> 
where
    Q: Hash + Eq + ?Sized + Send + Sync + ToOwned<Owned = K>,
    K: Hash + Eq + Borrow<Q>,
    S: BuildHasher,
 {
    flight: Larc<Flight<T>>,
    key: &'a Q,
    map: &'a HashMap<K, MapFlight<T>, S>,
}

impl<'a, Q, K, T, S>  InFlight<'a, Q, K, T, S>
where
    Q: Hash + Eq + ?Sized + Send + Sync + ToOwned<Owned = K>,
    K: Hash + Eq + Borrow<Q>,
    S: BuildHasher,
{
    pub fn get_flight(key: &'a Q, map: &'a HashMap<K, MapFlight<T>, S>, collector: &Arc<Collector>) -> Self {
        let mut next_flight = None; // This lets us move values out of the compute closure while using an insert operation.
        let pinned_map = map.pin();

        pinned_map.compute(key.to_owned(), |entry| {
            fn insert_flight<T>(collector: &Arc<Collector>, next_flight: &mut Option<Larc<Flight<T>>>) -> MapFlight<T> {
                let darc = Darc::new(Flight::with_collector(collector.clone()));
                *next_flight = darc.try_open(); // This can never fail since we havent locked the darc nor let any other threads access it.
                MapFlight::new(darc)
            }
            
            match entry { // every path here sets next_flight to Some.
                Some((_, map_flight)) => match map_flight.flight.try_open() {
                    Some(flight) => {
                        next_flight = Some(flight);
                        papaya::Operation::Abort(())
                    }
                    // if the flight is locked, it will never be unlocked and we can safely insert a new one and start working.
                    None => papaya::Operation::Insert(insert_flight(collector, &mut next_flight))
                }
                None => papaya::Operation::Insert(insert_flight(collector, &mut next_flight)),
            }
        });

        let flight = next_flight.expect("Shouldve set next_flight to Some");
        Self { flight, key, map }
    }
}


impl<Q, K, T, S>  InFlight<'_, Q, K, T, S>
where
    T: Clone,
    Q: Hash + Eq + ?Sized + Send + Sync + ToOwned<Owned = K>,
    K: Hash + Eq + Borrow<Q>,
    S: BuildHasher,
{
    /// Attempts to transition the flight to the next state.
    /// 
    /// Returns an enum signifying the state transition and what to do next.
    /// 
    /// # Panics
    /// Panics if the future is not available when leading.
    /// This should only happen if `next` is called after becoming a leader.
    pub fn next<F: Future<Output = Output> + Send, Output>(&self, fut: &mut Option<F>) -> Next<'_, T, F, Output> {
        let match_state = |state: &State<T>| match state {
            State::Uninit | State::LeaderDropped => Operation::Set(State::Running),
            State::Running => Operation::Abort(Next::Follow(Follower { in_flight: &self.flight })),
            State::LeaderFailed => Operation::Abort(Next::LeaderFailed),
            State::Success(val) => Operation::Abort(Next::Success(val.clone()))
        };
    
        // This lets us drop the Set returned values so we dont hold the cell_pinned() outside the function.
        match self.flight.cell_pinned().compute(match_state) {
            Compute::Set { .. } => {
                let fut = fut.take().expect("Future should be available when leading!");
                Next::Lead(Leader::new(fut, &self.flight))
            }
            Compute::Aborted(next) => next
        }
    }
}

impl<Q, K, T, S> Drop for InFlight<'_, Q, K, T, S>
where
    Q: Hash + Eq + ?Sized + Send + Sync + ToOwned<Owned = K>,
    K: Hash + Eq + Borrow<Q>,
    S: BuildHasher, 
{
    fn drop(&mut self) {
        // we can remove this flight from the map if we are the last remaining flight.
        if Larc::try_lock(&self.flight) {
            let _ = self.map.pin().remove_if(self.key, |_, flight| {
                Larc::ptr_eq(&self.flight, &flight.flight)
            });
        }
    }
}

pub(crate) enum Next<'a, T, F, Output>
where
    T: Clone,
    F: Future<Output = Output>,
{
    Lead(Leader<'a, T, F, Output>),
    Follow(Follower<'a, T>),
    LeaderFailed,
    Success(T)
}

pub(crate) struct Follower<'a, T> {
    in_flight: &'a Larc<Flight<T>>,
}

impl<T: Clone> Follower<'_, T> {
    pub fn wait(&self) -> EventListener {
        self.in_flight.wait()
    }

    // the state from the flight's cell is tied to the lifetime of a guard, so we need to move the guard to the caller's stack to give a reference.
    pub fn guard_state(&self) -> StateGuard<'_> {
        let guard = self.in_flight.result.guard();
        StateGuard { guard }
    }

    pub fn current_state<'g>(&self, guard: &'g StateGuard<'g>) -> &'g State<T> {
        self.in_flight.result.get(&guard.guard)
    }
}

pub(crate) struct StateGuard<'a> {
    guard: LocalGuard<'a>,
}

pub(crate) struct Flight<T> {
    result: ConcurrentCell<State<T>>,
    notify: BigNotify,
}

impl<T> Flight<T>  {
    pub fn with_collector(collector: Arc<Collector>) -> Self {
        Self {
            result: ConcurrentCell::with_collector(State::Uninit, collector),
            notify: BigNotify::new(),
        }
    }

    /// Sets the state of the flight and notifies waiters.
    pub fn set_state(&self, state: State<T>) {
        self.result.pin().set(state);
        self.notify.notify_listeners();
    }

    /// Updates the state of the map using the provided function and notifies waiters if the value is set.
    /// 
    /// Note that the closure may be retried in the event of concurrent updates.
    pub fn update<V>(&self, f: impl Fn(&State<T>) -> Operation<State<T>, V>) {
        let pinned = self.cell_pinned();
        if matches!(pinned.compute(f), Compute::Set { .. }) {
            // we only want to notify waiters if a new value was actually set.
            self.notify.notify_listeners();
        }
    }

    pub fn cell_pinned(&self) -> PinnedCell<'_, State<T>, LocalGuard<'_>> {
        self.result.pin()
    } 

    /// Returns an event listener that is notified when the state of the flight changes.
    pub fn wait(&self) -> EventListener {
        self.notify.listen()
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
        flight: &'a Larc<Flight<T>>,
    }
    
    impl<T, F, Output> PinnedDrop for Leader<'_, T, F, Output>
    where
        T: Clone,
        F: Future<Output = Output>,
    {
        fn drop(this: Pin<&mut Self>) {
            let this = this.project();
            // Try to lock here. If were the last Larc, the in_flight dropping will drop the flight.
            // To prevent dangling flights, we need to lock it before we transition to `LeaderDropped`.
            Larc::try_lock(this.flight); 
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
    pub fn new(fut: F, flight: &'a Larc<Flight<T>>) -> Self {
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

/// Based off tokio's [`BigNotify`](https://github.com/tokio-rs/tokio/blob/master/tokio/src/sync/watch.rs#L372).
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
    
    pub(super) fn notify_listeners(&self) {
        for notify in &self.inner {
            notify.notify(usize::MAX);
        }
    }
    
    pub(super) fn listen(&self) -> EventListener {
        let i = fastrand::usize(..8);
        self.inner[i].listen()
    }
}
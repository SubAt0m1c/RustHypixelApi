use std::{borrow::Borrow, hash::{BuildHasher, Hash}, pin::Pin, task::{Context, Poll, ready}};

use cusp::{Compute, Cell, Operation, PinnedCell};
use event_listener::{Event, EventListener};
use papaya::{HashMap, LocalGuard};
use pin_project_lite::pin_project;
use simple_defer::{Deferred, defer};

use crate::larc::{Darc, Larc};

pub struct MapFlight<T> {
    flight: Darc<Flight<T>>,
}

impl<T> MapFlight<T> {
    pub fn new(flight: Darc<Flight<T>>) -> Self {
        Self { flight }
    }

    pub fn same_flight(&self, map_flight: &InFlight<T>) -> bool {
        self.flight.ptr_eq(&map_flight.flight)
    }
}

pub(crate) struct InFlight<T> {
    flight: Larc<Flight<T>>,
}

impl<T> InFlight<T> {
    pub fn get_flight<'a, Q, K, S>(key: &'a Q, map: &'a HashMap<K, MapFlight<T>, S>) -> Self 
    where
        Q: Hash + Eq + ?Sized + Send + Sync + ToOwned<Owned = K>,
        K: Hash + Eq + Borrow<Q>,
        S: BuildHasher,
    {
        let mut next_flight = None; // This lets us move values out of the compute closure while using an insert operation.
        let pinned_map = map.pin();

        pinned_map.compute(key.to_owned(), |entry| {
            fn insert_flight<T>(next_flight: &mut Option<Larc<Flight<T>>>) -> MapFlight<T> {
                let darc = Darc::new(Flight::new());
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
                    None => papaya::Operation::Insert(insert_flight(&mut next_flight))
                }
                None => papaya::Operation::Insert(insert_flight(&mut next_flight)),
            }
        });

        let flight = next_flight.expect("Shouldve set next_flight to Some");
        Self { flight }
    }
    
    pub fn defered_remove<'a, Q, K, S>(&self, key: &'a Q, map: &'a HashMap<K, MapFlight<T>, S>) -> impl Deferred 
    where
        Q: Hash + Eq + ?Sized + Send + Sync + ToOwned<Owned = K>,
        K: Hash + Eq + Borrow<Q>,
        S: BuildHasher,
    {
        defer(|| {
            if Larc::try_lock(&self.flight) {
                let _ = map.pin().remove_if(key, |_, flight| {
                    flight.same_flight(self)
                });
            }
        })
    }
    /// Attempts to transition the flight to the next state.
    /// 
    /// Returns an enum signifying the state transition and what to do next.
    /// 
    /// # Panics
    /// Panics if the future is not available when leading.
    /// This should only happen if `next` is called after already becoming a leader.
    pub fn next<F: Future<Output = Output> + Send, Output>(&self, fut: &mut Option<F>) -> Next<'_, T, F, Output>
    where
        T: Clone
    {
        let match_state = |state: &State<T>| match state {
            State::Uninit | State::LeaderDropped => Operation::Set(State::Running),
            State::Running => Operation::Abort(Next::Follow(Follower { in_flight: &self.flight })),
            State::LeaderFailed => Operation::Abort(Next::LeaderFailed),
            State::Success(val) => Operation::Abort(Next::Success(val.clone()))
        };
    
        match self.flight.state_cell().compute(match_state) {
            Compute::Set { .. } => {
                let fut = fut.take().expect("Future should be available when leading!");
                Next::Lead(Leader::new(fut, &self.flight))
            }
            Compute::Aborted { value, ..} => value
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
    result: Cell<State<T>>,
    notify: BigNotify,
}

impl<T> Flight<T>  {
    pub fn new() -> Self {
        Self {
            result: Cell::new(State::Uninit),
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
        if self.state_cell().compute(f).is_set() {
            self.notify.notify_listeners();
        }
    }

    pub fn state_cell(&self) -> PinnedCell<'_, State<T>, LocalGuard<'_>> {
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
    fn new(fut: F, flight: &'a Larc<Flight<T>>) -> Self {
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
        
        let result = ready!(this.fut.poll(cx));
        match &result {
            Ok(v) => this.flight.set_state(State::Success(v.clone())),
            Err(_) => this.flight.set_state(State::LeaderFailed),
        }
        
        Poll::Ready(result)
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
        
        let val = ready!(this.fut.poll(cx));
        this.flight.set_state(State::Success(val.clone()));
        
        Poll::Ready(val)
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
use std::{pin::Pin, sync::{Arc, atomic::{AtomicU8, Ordering}}, task::{Context, Poll}};
use concurrent_cell::{Collector, ConcurrentCell};
use event_listener::{Event, EventListener};
use pin_project_lite::pin_project;

pub(crate) enum FlightType<T> {
    Follower(Arc<Flight<T>>),
    Leader(Arc<Flight<T>>),
}

impl<T> FlightType<T> {
    pub fn try_lead(flight: Arc<Flight<T>>, expected_state: State) -> Self {
        let leader = flight.try_acquire(expected_state);
        if leader {
            FlightType::Leader(flight)
        } else {
            FlightType::Follower(flight)
        }
    }
}

pub(crate) struct Flight<T> {
    state: AtomicU8,
    result: ConcurrentCell<Option<T>>,
    notify: BigNotify,
}

impl<T> Flight<T> {
    pub fn with_collector(collector: Arc<Collector>) -> Self {
        Self {
            state: AtomicU8::new(State::Uninit as u8),
            result: ConcurrentCell::with_collector(None, collector),
            notify: BigNotify::new(),
        }
    }
    
    pub fn try_acquire(&self, expected: State) -> bool {
        self.state.compare_exchange(expected as u8, State::Starting as u8, Ordering::Release, Ordering::Acquire).is_ok()
    }

    pub fn state(&self) -> State {
        State::try_from(self.state.load(Ordering::Acquire)).expect("State should never be invalid.")
    }
    
    fn update(&self, update: Update<T>) {
        match update {
            Update::LeaderDropped => self.state.store(State::LeaderDropped as u8, Ordering::Release),
            Update::LeaderFailed => self.state.store(State::LeaderFailed as u8, Ordering::Release),
            Update::Success(value) => {
                self.result.set(Some(value));
                self.state.store(State::Success as u8, Ordering::Release);
            }
        }

        self.notify.notify_waiters();
    }

    fn update_with(&self, modify: impl FnOnce(State) -> Option<Update<T>>) -> bool {
        let state = State::try_from(self.state.load(Ordering::Acquire)).expect("State should never be invalid.");
        if let Some(update) = modify(state) {
            self.update(update);
            true
        } else {
            false
        }
    }

    pub fn wait(&self) -> EventListener {
        self.notify.notified()
    }
}

impl<T: Clone> Flight<T> {
    pub fn value_cloned(&self) -> Option<T> {
        self.result.pin().get().clone()
    }
}

pub enum Update<T> {
    LeaderDropped,
    LeaderFailed,
    Success(T),
}

#[repr(u8)]
pub enum State {
    Uninit = 0,
    Starting = 1,
    LeaderDropped = 2,
    LeaderFailed = 3,
    Success = 4,
}

impl TryFrom<u8> for State {
    type Error = ();

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(Self::Uninit),
            1 => Ok(Self::Starting),
            2 => Ok(Self::LeaderDropped),
            3 => Ok(Self::LeaderFailed),
            4 => Ok(Self::Success),
            _ => Err(()),
        }
    }
}

pin_project! {
    pub(crate) struct Leader<T, F, Output>
    where
        T: Clone,
        F: Future<Output = Output>,
    {
        #[pin]
        fut: F,
        flight: Arc<Flight<T>>,
    }
    
    impl<T, F, Output> PinnedDrop for Leader<T, F, Output>
    where
        T: Clone,
        F: Future<Output = Output>,
    {
        fn drop(this: Pin<&mut Self>) {
            let this = this.project();
            let _ = this.flight.update_with(|s| {
                if matches!(s, State::Starting) {
                    Some(Update::LeaderDropped)
                } else {
                    None
                }
            });
        }
    }
}


impl<T, F, Output> Leader<T, F, Output>
where
    T: Clone,
    F: Future<Output = Output>,
{
    pub fn new(fut: F, flight: Arc<Flight<T>>) -> Self {
        Self { fut, flight }
    }
}

impl<T, E, F> Future for Leader<T, F, Result<T, E>>
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
                Ok(v) => this.flight.update(Update::Success(v.clone())),
                Err(_) => this.flight.update(Update::LeaderFailed),
            }
        }
        result
    }
}

#[allow(clippy::mismatching_type_param_order)]
impl<T, F> Future for Leader<T, F, T>
where
    T: Clone + Send + Sync,
    F: Future<Output = T>,
{
    type Output = T;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.project();
        let result = this.fut.poll(cx);
        if let Poll::Ready(val) = &result {
            this.flight.update(Update::Success(val.clone()));
        }
        result
    }
}

/// Heavily inspired by tokio's big notify in their watcher tasks.
/// Used to minimize contention.
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
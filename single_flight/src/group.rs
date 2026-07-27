use std::{borrow::Borrow, fmt::{self, Debug}, hash::{BuildHasher, Hash, RandomState}, marker::PhantomData, pin::pin, sync::Arc};

use concurrent_cell::Collector;
use papaya::HashMap;

use crate::{error::GroupWorkError, types::{Flight, FlightType, Leader, State}};

pub struct Group<K, T, E, S = RandomState> {
    collector: Arc<Collector>, // Collector for the ConcurrentCell's in the Flights.
    map: HashMap<K, Arc<Flight<T>>, S>,
    _marker: PhantomData<fn(E)>,
}

pub type DefaultGroup<T, E = ()> = Group<String, T, E>;

impl<K, T, E, S> Debug for Group<K, T, E, S> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Group").finish()
    }
}

impl<K, T, E, S> Default for Group<K, T, E, S>
where
    S: Default,
{
    fn default() -> Self {
        Self {
            collector: Arc::new(Collector::default()),
            map: HashMap::<K, Arc<Flight<T>>, S>::default(),
            _marker: PhantomData,
        }
    }
}

impl<K, T, E, S> Group<K, T, E, S>
where
    S: Default,
{
    #[must_use]
    pub fn new() -> Group<K, T, E, S> {
        Self::default()
    }
    
    #[must_use]
    pub fn with_hasher(hash_builder: S) -> Group<K, T, E, S> {
        Self {
            collector: Arc::new(Collector::default()),
            map: HashMap::with_hasher(hash_builder),
            _marker: PhantomData,
        }
    }    
}

impl<K, T, E, S> Group<K, T, E, S>
where
    T: Clone,
    K: Hash + Eq,
    S: BuildHasher,
{
    async fn work_inner<Q, F>(&self, key: &Q, fut: &mut Option<F>, is_retry: bool) -> Result<T, GroupWorkError<E>>
    where 
        Q: Hash + Eq + ?Sized + Send + Sync + ToOwned<Owned = K>,
        F: Future<Output = Result<T, E>> + Send,
        K: Borrow<Q>,
    {   
        let handler = {
            let map = self.map.pin();
            let flight = map.get_or_insert_with(key.to_owned(), || Arc::new(Flight::with_collector(self.collector.clone())));
            let expected = flight.state();
            match expected {
                State::Uninit | State::LeaderDropped => FlightType::try_lead(flight.clone(), expected),
                State::Starting => FlightType::Follower(flight.clone()),
                State::LeaderFailed => {
                    if is_retry {
                        return Err(GroupWorkError::LeaderFailed);
                    }
    
                    FlightType::try_lead(flight.clone(), expected)
                }
                State::Success => {
                    // Slow retries would trigger a new flight if we let them lead here.
                    // 
                    // We could make leaders unconditionally remove the flight from the map and never worry about stale entries,
                    // but that would make slow retries trigger a new flight.
                    if is_retry {
                        return Ok(flight.value_cloned().expect("State should be `Success`"))
                    }

                    // if this is a completely new worker, we can let them take over if this is stale.
                    FlightType::try_lead(flight.clone(), expected)
                }
            }
        };
        match handler {
            FlightType::Leader(flight) => {
                let leader = Leader::new(
                    fut.take().expect("Future should be available as leader"),
                    flight.clone(),
                );
                
                let result = leader.await;

                if !is_retry {
                    // Atomic remove in case another thread starts a new flight since we set it to success.
                    let _ = self.map.pin().remove_if(key, |_, existing| Arc::ptr_eq(existing, &flight));
                }
                
                result.map_err(GroupWorkError::Error)
            }
            FlightType::Follower(flight) => {
                loop {
                    let notified = pin!(flight.wait());

                    match flight.state() {
                        State::Starting | State::Uninit => notified.await,
                        State::LeaderDropped => return Err(GroupWorkError::LeaderDropped),
                        State::LeaderFailed => return Err(GroupWorkError::LeaderFailed),
                        State::Success => return Ok(flight.value_cloned().expect("State should be `Success`")),
                    }
                }
            }
        }
    }
    

    /// Executes the given function while ensuring only one is "in flight" at any given time.
    /// 
    /// Duplicate calls wait for the original call to complete and return the same value by cloning it.
    /// If the returned value is expensive, wrap it in an [`Arc`].
    /// 
    /// # Errors
    /// 
    /// If the function returns an `Err`
    /// - The leading caller will return `Err(Some(error))`
    /// - Non-leading callers will return `Err(None)`
    pub async fn work<Q, F>(&self, key: &Q, fut: F) -> Result<T, Option<E>>
    where
        Q: Hash + Eq + ?Sized + Send + Sync + ToOwned<Owned = K>,
        F: Future<Output = Result<T, E>> + Send,
        K: std::borrow::Borrow<Q>,
    {
        let mut fut_opt = Some(fut);
        let mut is_retry = false;

        loop {
            match self.work_inner(key, &mut fut_opt, is_retry).await {
                Err(GroupWorkError::LeaderDropped) => is_retry = true,
                Ok(val) => return Ok(val),
                Err(GroupWorkError::Error(err)) => return Err(Some(err)),
                Err(GroupWorkError::LeaderFailed) => return Err(None),
            }
        }
    }

    /// Removes entries left after a leader has dropped.
    /// 
    /// Dropped leaders will not remove their entries from the map,
    /// and this method will remove those stale entries.
    /// 
    /// If called while a follower is recovering a leader, late arriving
    /// retries may end up rerunning the work function.
    pub fn purge_stale(&self) {
        self.map.pin().retain(|_, flight| {
            matches!(flight.state(), State::Starting | State::Uninit)
        });
    }
}
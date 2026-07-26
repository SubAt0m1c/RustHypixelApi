use std::{borrow::Borrow, fmt::{self, Debug}, hash::{BuildHasher, Hash, RandomState}, marker::PhantomData, pin::pin, sync::Arc};

use papaya::HashMap;

use crate::{error::GroupWorkError, types::{Flight, FlightType, Leader, State}};

pub struct Group<K, T, E, S = RandomState> {
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
            let flight = map.get_or_insert_with(key.to_owned(), || Arc::new(Flight::default()));
            let expected = flight.state();
            match expected {
                State::Uninit | State::LeaderDropped => {
                    FlightType::try_lead(flight.clone(), expected)
                }
                State::Starting => FlightType::Follower(flight.clone()),
                State::LeaderFailed => {
                    if is_retry {
                        return Err(GroupWorkError::LeaderFailed);
                    }
                    
                    FlightType::try_lead(flight.clone(), expected)
                }
                State::Success => {
                    if is_retry {
                        return Ok(Arc::unwrap_or_clone(flight.value()));
                    }
                    
                    FlightType::try_lead(flight.clone(), expected)
                }
            }
        };
        match handler {
            FlightType::Leader(flight) => {
                let leader = Leader::new(
                    fut.take().expect("Future should be available as leader"),
                    flight,
                );
                
                let result = leader.await;
                if !is_retry {
                    self.map.pin().remove(key);
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
                        State::Success => return Ok(Arc::unwrap_or_clone(flight.value())),
                    }
                }
            }
        }
    }

    /// Executes the given function while ensuring only one is "in flight" at any given time.
    /// 
    /// Duplicate calls wait for the original call to complete and return the same value.
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
}
use std::{borrow::Borrow, fmt::{self, Debug}, hash::{BuildHasher, Hash, RandomState}, marker::PhantomData};

use papaya::HashMap;

use crate::{error::GroupWorkError, types::{Follower, InFlight, MapFlight, Next, State}};

pub struct Group<K, T, E, S = RandomState> {
    map: HashMap<K, MapFlight<T>, S>,
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
            map: HashMap::<K, MapFlight<T>, S>::default(),
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
}

impl<K, T, E, S> Group<K, T, E, S>
where
    T: Clone,
    K: Hash + Eq,
    S: BuildHasher,
{
    /// Creates a new `Group` with the given hash builder.
    #[must_use]
    pub fn with_hasher(hash_builder: S) -> Group<K, T, E, S> {
        Self {
            map: HashMap::with_hasher(hash_builder),
            _marker: PhantomData,
        }
    }
    
    /// Executes the given function while ensuring only one is "in flight" at any given time.
    /// 
    /// Duplicate calls wait for the original call to complete and return the same value by 
    /// cloning it. If the returned value is expensive, wrap it in an [`Arc`].
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
        match self.work_inner(key, &mut fut_opt).await {
            Ok(val) => Ok(val),
            Err(GroupWorkError::Error(err)) => Err(Some(err)),
            Err(GroupWorkError::LeaderFailed) => Err(None),
        }
    }
    
    async fn work_inner<Q, F>(&self, key: &Q, fut: &mut Option<F>) -> Result<T, GroupWorkError<E>>
    where 
        Q: Hash + Eq + ?Sized + Send + Sync + ToOwned<Owned = K>,
        F: Future<Output = Result<T, E>> + Send,
        K: Borrow<Q>,
    {   
        // The lifetime of this InFlight holds the flight alive in the map. If we are the last remaining flight, 
        // we will drop it AND remove it from the map.
        let current_flight = InFlight::get_flight(key, &self.map);

        'next_state: loop {
            match current_flight.next(fut) {
                Next::Lead(leader) => {
                    let result = leader.await;

                    // Unconditionally remove here. Leader.await succeeding will automatically set the state to success and any flights that join
                    // before the flight is removed can happily take that non-stale success out. Late retryers own their own flight, and wont need
                    // it to remain in the map.
                    let _ = self.map.pin().remove_if(key, |_, existing| existing.same_flight(&current_flight)); 
                    return result.map_err(GroupWorkError::Error)
                }
                Next::Follow(follower) => {
                    loop {
                        let wait_for_leader = follower.wait();
                        // weird method call here because rust_analyzer struggles to figure its type T.
                        match Follower::<T>::current_state(&follower, &follower.guard_state()) {
                            State::Uninit | State::Running => {} // exit the match to await so we dont await while holding the state guard.
                            State::LeaderDropped => continue 'next_state,
                            State::LeaderFailed => return Err(GroupWorkError::LeaderFailed),
                            State::Success(val) => return Ok(val.clone()),
                        }
                        wait_for_leader.await;                        
                    }
                }
                Next::LeaderFailed => return Err(GroupWorkError::LeaderFailed),
                Next::Success(val) => return Ok(val)
            }
        }
    }
}
use std::time::Duration;

// this is sized to be the same as an enum discriminent so a UuidKey would be the same size as an enum with a uuid field.
// theoretically we may be able to reduce that to just a uuid size since the discriminent is embedded in the uuid, but
// i was unable to think of a way to get the uuidkey to include cache ttl for the cache expiry trait. We could hardcode it
// with a match statement but that seems lame.
#[derive(PartialEq, Eq, PartialOrd, Ord, Hash, Clone, Copy)]
pub struct Expires {
    seconds: u8
}

impl Expires {
    pub fn new(seconds: u8) -> Self {
        Self {
            seconds
        }
    }

    pub fn as_duration(&self) -> Duration {
        Duration::from_secs(self.seconds as u64)
    }
}
use serde::Serialize;
use uuid::Uuid;

#[derive(Hash, PartialEq, Eq, PartialOrd, Ord, Clone, Copy, Serialize)]
pub enum CacheKey {
    Secrets(Uuid),
    Profile(Uuid),
}

impl CacheKey {
    pub fn uuid(&self) -> Uuid {
        match self {
            Self::Profile(id) => *id,
            Self::Secrets(id) => *id,
        }
    }

    pub fn hypixel_url(&self) -> String {
        match self {
            CacheKey::Profile(id) => format!("https://api.hypixel.net/v2/skyblock/profiles?uuid={}", id),
            CacheKey::Secrets(id) => format!("https://api.hypixel.net/v2/player?uuid={}", id),
        }
    }
}
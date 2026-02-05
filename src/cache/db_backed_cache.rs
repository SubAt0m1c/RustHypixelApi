use crate::cache::moka_cache::MokaCache;

pub struct DbBackedCache {
    cache: MokaCache
}
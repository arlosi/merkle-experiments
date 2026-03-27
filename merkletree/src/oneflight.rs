//! An async caching layer.
//! 
//! Evaluates an async function f for a given key if it's not already in the cache.
//! 
//! Ensures that the function is only evaulated once per key, even if multiple requests are in flight.

use std::{collections::HashMap, hash::Hash, sync::Arc};

use futures::lock::Mutex;
use std::sync::Mutex as SyncMutex;

pub struct OneFlightCache<K, V> {
    cache: SyncMutex<HashMap<K, Arc<Mutex<Option<Arc<V>>>>>>
}

impl<K, V> OneFlightCache<K, V>
where K: Hash + Eq + Clone,
      V: Send + Sync,
{
    pub fn new() -> Self {
        Self {
            cache: Default::default()
        }
    }

    pub async fn get<F, E>(&self, key: &K, f: F) -> Result<Arc<V>, E>
    where
        F: AsyncFnOnce(&K) -> Result<V, E>,
    {
        let entry = {
            // Acquire the global lock to ensure only one key is inserted.
            let mut cache = self.cache.lock().unwrap();
            cache.entry(key.clone()).or_default().clone()
        };
        // Acquire the key lock to ensure future is only executed once.
        let mut guard = entry.lock().await;
        if let Some(v) = &*guard {
            Ok(v.clone())
        } else {
            // Run the function.
            let result = f(key).await.map(Arc::new);
            if let Ok(v) = &result {
                // Put it in the cache if successful.
                *guard = Some(v.clone());
            }
            result
        }
    }
}

#[cfg(test)]
mod tests{
    use std::sync::{Arc, atomic::AtomicU32};

    use crate::oneflight::OneFlightCache;

    struct Incrementer {
        i: AtomicU32,
        of: OneFlightCache<u32, u32>,
    }

    impl Incrementer {
        async fn inc_uncached(&self, k: &u32) -> Result<u32, ()> {
            self.i.fetch_add(1, std::sync::atomic::Ordering::AcqRel);
            Ok(k + 1)
        }

        fn new() -> Self {
            Self {
                i: Default::default(),
                of: OneFlightCache { cache: Default::default() },
            }
        }

        async fn inc(&self, k: &u32) -> Result<Arc<u32>, ()> {
            self.of.get(k, async |k| self.inc_uncached(k).await).await
        }
    }

    #[test]
    fn basic() -> anyhow::Result<()> {
        futures::executor::block_on(async {
            let i = Incrementer::new();

            assert_eq!(*i.inc(&0).await.unwrap(), 1);
            assert_eq!(*i.inc(&1).await.unwrap(), 2);
            Ok(())
        })
    }
}
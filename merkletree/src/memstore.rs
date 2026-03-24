use futures::channel::oneshot;
use std::{
    collections::{HashMap, HashSet},
    sync::{Arc, Mutex},
};

use tracing::{debug, trace};

use crate::Result;
use crate::{Error, Hash, ObjectStore};

pub struct MemoryStore {
    objects: Mutex<HashMap<(Hash, bool), Vec<u8>>>,
    index: Mutex<Option<Hash>>,
}

impl Default for MemoryStore {
    fn default() -> Self {
        Self::new()
    }
}

impl MemoryStore {
    pub fn new() -> Self {
        Self {
            objects: Mutex::new(HashMap::new()),
            index: Mutex::new(None),
        }
    }
}

impl ObjectStore for MemoryStore {
    async fn write(&self, hash: &Hash, data: &[u8], is_leaf: bool) -> Result<()> {
        let mut objs = self.objects.lock().unwrap();
        objs.entry((hash.clone(), is_leaf))
            .or_insert_with(|| data.to_vec());
        Ok(())
    }

    async fn read(&self, hash: &Hash, is_leaf: bool) -> Result<Vec<u8>> {
        let objs = self.objects.lock().unwrap();
        match objs.get(&(hash.clone(), is_leaf)) {
            Some(d) => Ok(d.clone()),
            None => Err(Error::NotFound),
        }
    }

    async fn delete(&self, hash: &Hash, is_leaf: bool) -> Result<()> {
        let mut objs = self.objects.lock().unwrap();
        objs.remove(&(hash.clone(), is_leaf));
        Ok(())
    }

    async fn enumerate_all(&self) -> Result<HashSet<(Hash, bool)>> {
        let objs = self.objects.lock().unwrap();
        Ok(objs.keys().cloned().collect())
    }

    fn load_index(&self) -> Result<Option<Hash>> {
        let idx = self.index.lock().unwrap();
        Ok(idx.clone())
    }

    fn save_index(&self, root_hash: &Hash) -> Result<()> {
        let mut idx = self.index.lock().unwrap();
        *idx = Some(root_hash.clone());
        Ok(())
    }
}

pub struct CacheStore<T: ObjectStore> {
    inner: T,
    cache: Mutex<HashMap<Hash, Vec<u8>>>,
    in_flight: Mutex<HashMap<Hash, Vec<oneshot::Sender<Arc<Result<Vec<u8>>>>>>>,
}

impl<T: ObjectStore> CacheStore<T> {
    pub fn new(inner: T) -> Self {
        Self {
            inner,
            cache: Mutex::new(HashMap::new()),
            in_flight: Mutex::new(HashMap::new()),
        }
    }
}

impl<T: ObjectStore + Sync> CacheStore<T> {
    pub fn clear(&self) {
        self.cache.lock().unwrap().clear();
    }
}

impl<T: ObjectStore + Sync> ObjectStore for CacheStore<T> {
    async fn write(&self, hash: &Hash, data: &[u8], is_leaf: bool) -> Result<()> {
        {
            let mut cache = self.cache.lock().unwrap();
            cache.insert(hash.clone(), data.to_vec());
        }
        self.inner.write(hash, data, is_leaf).await
    }

    async fn read(&self, hash: &Hash, is_leaf: bool) -> Result<Vec<u8>> {
        // Fast path: cached
        {
            let cache = self.cache.lock().unwrap();
            if let Some(data) = cache.get(hash) {
                trace!(hash = hash.0, "cache hit");
                return Ok(data.clone());
            }
        }

        // Check if there's an in-flight request. If so, register and wait for its result.
        let rx_opt = {
            let mut in_flight = self.in_flight.lock().unwrap();
            if in_flight.contains_key(hash) {
                debug!("deferring in-flight request");
                let (tx, rx) = oneshot::channel::<Arc<Result<Vec<u8>>>>();
                in_flight.get_mut(hash).unwrap().push(tx);
                Some(rx)
            } else {
                // Mark as in-flight with an empty waiter list and become the leader.
                in_flight.insert(hash.clone(), Vec::new());
                None
            }
        };

        if let Some(rx) = rx_opt {
            match rx.await {
                Ok(arc_res) => {
                    return match &*arc_res {
                        Ok(d) => Ok(d.clone()),
                        Err(e) => Err(e.clone()),
                    };
                }
                Err(_) => return Err(crate::Error::Io("channel".to_string())),
            }
        }

        // Leader: perform backend read once.
        let result = self.inner.read(hash, is_leaf).await;

        // Cache successful results.
        if let Ok(ref data) = result {
            let mut cache = self.cache.lock().unwrap();
            cache.insert(hash.clone(), data.clone());
        }

        // Notify all waiters.
        let arc_res = Arc::new(result.clone());
        let waiters = {
            let mut in_flight = self.in_flight.lock().unwrap();
            in_flight.remove(hash).unwrap_or_default()
        };
        for tx in waiters {
            let _ = tx.send(arc_res.clone());
        }

        // Return leader's result
        match &*arc_res {
            Ok(d) => Ok(d.clone()),
            Err(e) => Err(e.clone()),
        }
    }

    async fn delete(&self, hash: &Hash, is_leaf: bool) -> Result<()> {
        {
            let mut cache = self.cache.lock().unwrap();
            cache.remove(hash);
        }
        self.inner.delete(hash, is_leaf).await
    }

    async fn enumerate_all(&self) -> Result<HashSet<(Hash, bool)>> {
        self.inner.enumerate_all().await
    }

    fn load_index(&self) -> Result<Option<Hash>> {
        self.inner.load_index()
    }

    fn save_index(&self, root_hash: &Hash) -> Result<()> {
        self.inner.save_index(root_hash)
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use crate::Store;

    use super::*;

    #[test]
    fn test_memory_store() -> Result<()> {
        futures::executor::block_on(async {
            let mut store = Store::new(MemoryStore::new(), 2, 4);
            store.load()?;
            store.put_object("test", b"hello world").await?;
            let content = store.get_file("test").await?.unwrap();
            store.save()?;
            assert_eq!(content, b"hello world");

            Ok(())
        })
    }

    #[test]
    fn multiple_files() -> Result<()> {
        futures::executor::block_on(async {
            let mut store = Store::new(MemoryStore::new(), 2, 4);
            store.load()?;
            store
                .overwrite(&vec![("test".to_string(), PathBuf::from("Cargo.toml"))])
                .await?;
            store.save()?;
            let content = store.get_file("test").await?.unwrap();
            assert_eq!(content, std::fs::read("Cargo.toml")?);

            Ok(())
        })
    }
}

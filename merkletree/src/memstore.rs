use std::{
    collections::{HashMap, HashSet},
    convert::Infallible,
    sync::Mutex,
};

use crate::{GCObjectStore, Hash, ReadObjectStore, WriteObjectStore};

pub struct MemoryStore {
    objects: Mutex<HashMap<(Hash, bool), Vec<u8>>>,
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
        }
    }
}

impl ReadObjectStore for MemoryStore {
    type E = Infallible;

    async fn read(&self, hash: &str, is_leaf: bool) -> Result<Option<Vec<u8>>, Self::E> {
        let objs = self.objects.lock().unwrap();
        Ok(objs.get(&(Hash(hash.to_string()), is_leaf)).cloned())
    }
}

impl WriteObjectStore for MemoryStore {
    async fn write(&self, hash: &str, data: Vec<u8>, is_leaf: bool) -> Result<(), Self::E> {
        let mut objs = self.objects.lock().unwrap();
        objs.entry((Hash(hash.to_string()), is_leaf))
            .or_insert_with(|| data);
        Ok(())
    }
}

impl GCObjectStore for MemoryStore {
    async fn delete(&self, hash: &str, is_leaf: bool) -> Result<(), Self::E> {
        let mut objs = self.objects.lock().unwrap();
        objs.remove(&(Hash(hash.to_string()), is_leaf));
        Ok(())
    }

    async fn enumerate_all(&self) -> Result<HashSet<(String, bool)>, Self::E> {
        let objs = self.objects.lock().unwrap();
        Ok(objs.keys().map(|(a, b)| (a.0.clone(), *b)).collect())
    }
}

#[cfg(test)]
mod tests {
    use crate::MerkleStore;

    use super::*;

    #[test]
    fn test_memory_store() {
        futures::executor::block_on(async {
            let mut store = MerkleStore::new(MemoryStore::new());
            store.configure(None, 2, 4).await;
            store
                .put_object("test", b"hello world".to_vec())
                .await
                .unwrap();
            let content = store.get_file("test").await.unwrap().unwrap();
            assert_eq!(content, b"hello world");
        })
    }

    #[test]
    fn multiple_files() {
        futures::executor::block_on(async {
            let mut store = MerkleStore::new(MemoryStore::new());
            store.configure(None, 2, 4).await;
            store
                .overwrite(vec![&"test".to_string()], |_| Ok(vec![1]))
                .await
                .unwrap();
            let content = store.get_file("test").await.unwrap().unwrap();
            assert_eq!(content, [1]);
        })
    }
}

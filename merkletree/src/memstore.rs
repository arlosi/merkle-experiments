use std::{
    collections::{HashMap, HashSet},
    convert::Infallible,
    sync::Mutex,
};

use crate::{GCObjectStore, TreeReader, TreeWriter, bitslice::ContentHash};

pub struct MemoryStore {
    data: Mutex<HashMap<ContentHash, Vec<u8>>>,
    tree: Mutex<HashMap<ContentHash, Vec<u8>>>,
}

impl Default for MemoryStore {
    fn default() -> Self {
        Self::new()
    }
}

impl MemoryStore {
    pub fn new() -> Self {
        Self {
            data: Mutex::new(HashMap::new()),
            tree: Mutex::new(HashMap::new()),
        }
    }
}

impl TreeReader for MemoryStore {
    type Error = Infallible;

    async fn read(
        &self,
        hash: &ContentHash,
        is_leaf: bool,
    ) -> Result<Option<Vec<u8>>, Self::Error> {
        if is_leaf {
            Ok(self.data.lock().unwrap().get(hash).cloned())
        } else {
            Ok(self.tree.lock().unwrap().get(hash).cloned())
        }
    }
}

impl TreeWriter for MemoryStore {
    async fn write(
        &self,
        hash: &ContentHash,
        data: Vec<u8>,
        is_leaf: bool,
    ) -> Result<(), Self::Error> {
        if is_leaf {
            self.data.lock().unwrap().insert(hash.clone(), data);
        } else {
            self.tree.lock().unwrap().insert(hash.clone(), data);
        }
        Ok(())
    }
}

impl GCObjectStore for MemoryStore {
    async fn delete(&self, hash: &ContentHash, is_leaf: bool) -> Result<(), Self::Error> {
        if is_leaf {
            self.data.lock().unwrap().remove(hash);
        } else {
            self.tree.lock().unwrap().remove(hash);
        }
        Ok(())
    }

    async fn enumerate_all(&self) -> Result<HashSet<(ContentHash, bool)>, Self::Error> {
        let mut result = HashSet::new();
        for k in self.tree.lock().unwrap().keys() {
            result.insert((k.clone(), false));
        }
        for k in self.data.lock().unwrap().keys() {
            result.insert((k.clone(), true));
        }
        Ok(result)
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

            for i in 0..100 {
                store
                    .put_object(&format!("node:{i}"), format!("data:{i}").into_bytes())
                    .await
                    .unwrap();
            }
            store.commit().await.unwrap();

            for i in 100..200 {
                store
                    .put_object(&format!("node:{i}"), format!("data:{i}").into_bytes())
                    .await
                    .unwrap();
            }

            for i in 0..200 {
                assert_eq!(
                    store.get_file(&format!("node:{i}")).await.unwrap().unwrap(),
                    format!("data:{i}").into_bytes()
                );
            }
        })
    }

    // #[test]
    // fn multiple_files() {
    //     futures::executor::block_on(async {
    //         let mut store = MerkleStore::new(MemoryStore::new());
    //         store.configure(None, 2, 4).await;
    //         store
    //             .overwrite(vec![&"test".to_string()], |_| Ok(vec![1]))
    //             .await
    //             .unwrap();
    //         let content = store.get_file("test").await.unwrap().unwrap();
    //         assert_eq!(content, [1]);
    //     })
    // }
}

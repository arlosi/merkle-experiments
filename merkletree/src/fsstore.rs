use std::{
    collections::HashSet,
    path::{Path, PathBuf},
};

use tracing::trace;

use crate::{GCObjectStore, ReadObjectStore, WriteObjectStore};

pub struct FsStore {
    root: PathBuf,
}

type IoResult<T> = std::result::Result<T, std::io::Error>;

const TREE: &'static str = "tree";
const DATA: &'static str = "data";

impl FsStore {
    pub fn new(root: impl AsRef<Path>) -> IoResult<Self> {
        let root = root.as_ref().to_path_buf();
        std::fs::create_dir_all(root.join(TREE))?;
        std::fs::create_dir_all(root.join(DATA))?;
        Ok(Self { root })
    }

    fn object_path(&self, hash: &str, is_leaf: bool) -> PathBuf {
        if is_leaf {
            self.root.join(DATA).join(&hash[0..2]).join(&hash[2..])
        } else {
            self.root.join(TREE).join(hash)
        }
    }
}

impl ReadObjectStore for FsStore {
    type E = std::io::Error;

    async fn read(&self, hash: &str, is_leaf: bool) -> IoResult<Option<Vec<u8>>> {
        let path = self.object_path(&hash, is_leaf);
        trace!("Reading object {}", path.display());
        let data = match std::fs::read(&path) {
            Ok(data) => Some(data),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => None,
            Err(e) => return Err(e.into()),
        };

        Ok(data)
    }
}

impl WriteObjectStore for FsStore {
    async fn write(&self, hash: &str, data: Vec<u8>, is_leaf: bool) -> IoResult<()> {
        let path = self.object_path(&hash, is_leaf);
        std::fs::create_dir_all(path.parent().unwrap())?;
        if !path.exists() {
            std::fs::write(&path, data)?;
        }
        Ok(())
    }
}

impl GCObjectStore for FsStore {
    async fn delete(&self, hash: &str, is_leaf: bool) -> IoResult<()> {
        let path = self.object_path(&hash, is_leaf);
        std::fs::remove_file(&path)?;
        Ok(())
    }

    async fn enumerate_all(&self) -> IoResult<HashSet<(String, bool)>> {
        let mut all = HashSet::new();
        let objects_dir = self.root.join(TREE);
        for entry in std::fs::read_dir(objects_dir)? {
            let entry = entry?;
            let hash = entry.file_name().to_str().unwrap().to_string();
            all.insert((hash, false));
        }
        let data_dir = self.root.join(DATA);
        for entry in std::fs::read_dir(&data_dir)? {
            let pfx = entry?;
            if pfx.metadata()?.is_dir() {
                for entry in std::fs::read_dir(pfx.path())? {
                    let hash = format!(
                        "{}{}",
                        pfx.file_name().to_str().unwrap(),
                        entry?.file_name().to_str().unwrap()
                    );
                    all.insert((hash, true));
                }
            }
        }
        Ok(all)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::executor::block_on;
    use std::fs;
    use std::path::PathBuf;

    fn temp_dir() -> PathBuf {
        use std::sync::atomic::{AtomicU64, Ordering};
        static COUNTER: AtomicU64 = AtomicU64::new(0);
        let count = COUNTER.fetch_add(1, Ordering::SeqCst);
        let mut dir = PathBuf::from("target").join("test-tmp");
        dir.push(format!("fsstore_test_{}", count));
        let _ = fs::remove_dir_all(&dir); // Clean up any previous test
        fs::create_dir_all(&dir).unwrap();
        dir
    }

    #[test]
    fn test_fsstore_new() {
        let temp = temp_dir();
        let store = FsStore::new(&temp);
        assert!(store.is_ok());
        let store = store.unwrap();
        assert!(store.root.join(TREE).exists());
        assert!(store.root.join(DATA).exists());
    }

    #[test]
    fn test_write_read_leaf() {
        let temp = temp_dir();
        let store = FsStore::new(&temp).unwrap();
        let hash = "test_hash".to_string();
        let data = b"hello world";

        block_on(async {
            store.write(&hash, data.to_vec(), true).await.unwrap();
            let read_data = store.read(&hash, true).await.unwrap().unwrap();
            assert_eq!(read_data, data);
        });
    }

    #[test]
    fn test_write_read_non_leaf() {
        let temp = temp_dir();
        let store = FsStore::new(&temp).unwrap();
        let hash = "test_hash".to_string();
        let data = b"{\"key\": \"value\"}";

        block_on(async {
            store.write(&hash, data.to_vec(), false).await.unwrap();
            let read_data = store.read(&hash, false).await.unwrap().unwrap();
            assert_eq!(read_data, data);
        });
    }

    #[test]
    fn test_delete() {
        let temp = temp_dir();
        let store = FsStore::new(&temp).unwrap();
        let hash = "test_hash".to_string();
        let data = b"hello world";

        block_on(async {
            store.write(&hash, data.to_vec(), true).await.unwrap();
            let read_data = store.read(&hash, true).await.unwrap().unwrap();
            assert_eq!(read_data, data);

            store.delete(&hash, true).await.unwrap();
            let result = store.read(&hash, true).await.unwrap();
            assert!(result.is_none());
        });
    }

    #[test]
    fn test_enumerate_all() {
        let temp = temp_dir();
        let store = FsStore::new(&temp).unwrap();
        let hash1 = "hash1".to_string();
        let hash2 = "hash2".to_string();
        let data1 = b"data1";
        let data2 = b"data2";

        block_on(async {
            store.write(&hash1, data1.to_vec(), true).await.unwrap();
            store.write(&hash2, data2.to_vec(), false).await.unwrap();

            let all = store.enumerate_all().await.unwrap();
            assert_eq!(all.len(), 2);
            assert!(all.contains(&(hash1.clone(), true)));
            assert!(all.contains(&(hash2.clone(), false)));
        });
    }

    #[test]
    fn test_object_path() {
        let temp = temp_dir();
        let store = FsStore::new(&temp).unwrap();

        // Test leaf path
        let leaf_path = store.object_path("abcd", true);
        assert_eq!(leaf_path, temp.join(DATA).join("ab").join("cd"));

        // Test non-leaf path
        let non_leaf_path = store.object_path("abcd", false);
        assert_eq!(non_leaf_path, temp.join(TREE).join("abcd"));
    }
}

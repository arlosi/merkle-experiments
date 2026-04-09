use futures::StreamExt;
use object_store::gcp::GoogleCloudStorage;
use object_store::path::Path;
use object_store::{ObjectMeta, ObjectStore, ObjectStoreExt};
use std::collections::HashSet;

use merkletree::{ContentHash, TreeEnumerator, TreeReader, TreeWriter};

#[derive(Clone, Debug)]
pub struct GcpStore<'a> {
    inner: &'a GoogleCloudStorage,
}

impl<'a> GcpStore<'a> {
    /// Build a connection from environment variables using `object_store`.
    ///
    /// Required fields in environment (or via builder config):
    /// - `GOOGLE_CLOUD_PROJECT` / service account credentials
    /// - bucket name can be set by `bucket` argument.
    pub fn new(store: &'a GoogleCloudStorage) -> Self {
        Self { inner: store }
    }

    fn object_path(hash: &ContentHash, is_leaf: bool) -> Path {
        let hash = hash.to_string();
        if is_leaf {
            // leaf objects are sharded like fsstore: data/{prefix2}/{rest}
            let prefix = &hash[0..2];
            let suffix = &hash[2..];
            Path::from(format!("merkle/data/{}/{}", prefix, suffix))
        } else {
            Path::from(format!("merkle/tree/{}", hash))
        }
    }
}

impl TreeReader for GcpStore<'_> {
    type Error = object_store::Error;

    async fn read(
        &self,
        hash: &ContentHash,
        is_leaf: bool,
    ) -> Result<Option<Vec<u8>>, Self::Error> {
        let path = Self::object_path(hash, is_leaf);
        match self.inner.get(&path).await {
            Ok(result) => {
                let bytes = result.bytes().await?;
                Ok(Some(bytes.to_vec()))
            }
            Err(err) => match err {
                object_store::Error::NotFound { .. } => Ok(None),
                other => Err(other),
            },
        }
    }
}

impl TreeWriter for GcpStore<'_> {
    async fn write(
        &self,
        hash: &ContentHash,
        data: Vec<u8>,
        is_leaf: bool,
    ) -> Result<(), Self::Error> {
        let path = Self::object_path(hash, is_leaf);
        let result = self.inner.put(&path, data.into()).await;
        if let Err(e) = &result {
            eprintln!("{e:?}");
        }
        result?;
        Ok(())
    }

    async fn delete(&self, hash: &ContentHash, is_leaf: bool) -> Result<(), Self::Error> {
        let path = Self::object_path(hash, is_leaf);
        self.inner.delete(&path).await?;
        Ok(())
    }
}

impl TreeEnumerator for GcpStore<'_> {
    async fn enumerate_all(&self) -> Result<HashSet<(ContentHash, bool)>, Self::Error> {
        let mut all = HashSet::new();

        let tree_prefix = Path::from("merkle/tree");
        let mut tree_stream = self.inner.list(Some(&tree_prefix));
        while let Some(entry) = tree_stream.next().await {
            let object_meta: ObjectMeta = entry?;
            let path = object_meta.location.as_ref();
            if let Some(hash_part) = path.strip_prefix("merkle/tree/") {
                let hashed = ContentHash::try_from(hash_part).unwrap();
                all.insert((hashed, false));
            }
        }

        let data_prefix = Path::from("merkle/data");
        let mut data_stream = self.inner.list(Some(&data_prefix));
        while let Some(entry) = data_stream.next().await {
            let object_meta: ObjectMeta = entry?;
            let path = object_meta.location.as_ref();

            if let Some(rest) = path.strip_prefix("merkle/data/") {
                if let Some((prefix, suffix)) = rest.split_once('/') {
                    let hash = format!("{}{}", prefix, suffix);
                    if let Ok(hash) = ContentHash::try_from(hash.as_str()) {
                        all.insert((hash, true));
                    }
                }
            }
        }

        Ok(all)
    }
}

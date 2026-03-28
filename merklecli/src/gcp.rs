use futures_util::StreamExt;
use object_store::gcp::GoogleCloudStorage;
use object_store::path::Path;
use object_store::{
    BackoffConfig, ObjectMeta, ObjectStore, ObjectStoreExt, Result as ObjectStoreResult,
    RetryConfig,
};
use std::collections::HashSet;
use std::time::Duration;

use merkletree::{GCObjectStore, ReadObjectStore, WriteObjectStore};

#[derive(Clone, Debug)]
pub struct GcpStore {
    inner: GoogleCloudStorage,
}

impl GcpStore {
    /// Build a connection from environment variables using `object_store`.
    ///
    /// Required fields in environment (or via builder config):
    /// - `GOOGLE_CLOUD_PROJECT` / service account credentials
    /// - bucket name can be set by `bucket` argument.
    pub fn new() -> ObjectStoreResult<Self> {
        let store = object_store::gcp::GoogleCloudStorageBuilder::from_env()
            .with_retry(RetryConfig {
                backoff: BackoffConfig {
                    init_backoff: Duration::from_secs(1),
                    max_backoff: Duration::from_secs(60),
                    base: 2.0,
                },
                max_retries: 5,
                retry_timeout: Duration::from_secs(600),
            })
            .build()?;
        Ok(Self { inner: store })
    }

    fn object_path(hash: &str, is_leaf: bool) -> Path {
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

impl ReadObjectStore for GcpStore {
    type E = object_store::Error;

    async fn read(&self, hash: &str, is_leaf: bool) -> Result<Option<Vec<u8>>, Self::E> {
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

impl WriteObjectStore for GcpStore {
    async fn write(&self, hash: &str, data: Vec<u8>, is_leaf: bool) -> Result<(), Self::E> {
        let path = Self::object_path(hash, is_leaf);
        let result = self.inner.put(&path, data.into()).await;
        if let Err(e) = &result {
            eprintln!("{e:?}");
        }
        result?;
        Ok(())
    }
}

impl GCObjectStore for GcpStore {
    async fn delete(&self, hash: &str, is_leaf: bool) -> Result<(), Self::E> {
        let path = Self::object_path(hash, is_leaf);
        self.inner.delete(&path).await?;
        Ok(())
    }

    async fn enumerate_all(&self) -> Result<HashSet<(String, bool)>, Self::E> {
        let mut all = HashSet::new();

        let tree_prefix = Path::from("tree");
        let mut tree_stream = self.inner.list(Some(&tree_prefix));
        while let Some(entry) = tree_stream.next().await {
            let object_meta: ObjectMeta = entry?;
            let path = object_meta.location.as_ref();
            if let Some(hash_part) = path.strip_prefix("tree/") {
                all.insert((hash_part.to_string(), false));
            }
        }

        let data_prefix = Path::from("data");
        let mut data_stream = self.inner.list(Some(&data_prefix));
        while let Some(entry) = data_stream.next().await {
            let object_meta: ObjectMeta = entry?;
            let path = object_meta.location.as_ref();
            if let Some(rest) = path.strip_prefix("data/") {
                if let Some((prefix, suffix)) = rest.split_once('/') {
                    let hashed = format!("{}{}", prefix, suffix);
                    all.insert((hashed, true));
                }
            }
        }

        Ok(all)
    }
}

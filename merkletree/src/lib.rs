//! Merkle-tree based object store with fixed tree structure determined by the server.
//! * Objects are stored by the content hash.
//! * Index nodes are stored as JSON-serialized maps from key prefixes to child node hashes.
//! * The tree has fixed depth and breadth when written.
//! * Readers can handle tree structure updates. This leaves us free to adjust later if needed.
//! * Logical names are mapped to content hashes via the Merkle tree.
//! * The tree is internally consistant for a given root hash. No tearing can occur.
//! * Multiple root hashes can be valid at the same time. Unreachable objects can be GC'ed.
//!
//! Since all file names are content-based, they can be cached forever without needing invalidations.
//! (except the root).
//!
//! Hash function: SHA256 + base64-encode (43 chars).
//!
//! Lookup process:
//! 1. Load the root node hash from somewhere (config.json?).
//! 2. Validate that the root node hash using TUF (not defined how that works here).
//! 3. Hash the crate name to get the name hash.
//! 4. Fetch the root node from the index (using it's hash).
//! 5. Traverse the prefix tree until we find the content.
//!    Keys in each level of the tree use a fixed number of characters from the hash as the prefix.
//!    
//! Nodes in the merkle tree are maps from hash-prefixes to child node hashes.
//!
//! Let's say the crate name is "foo", and the string "foo" hashes to "abcdefghij8fCrf9ppASRQqYAuBqOSCWTflF3tXiIvS".
//!
//! We load the root node, and look for the prefix "a" (the first char of the hash) in the map.
//! Note: All keys in the map are the same length.
//! ```json
//! {
//!    "a": "8xB2EYWrEahgCbqVEwu6QdmoTg-FpvQglfe8tyjAk6I",
//!    "b": "183L6zfnteCe94yQt7R_V7qtOov8_aIs533d_iR-qFA",
//!    "c": "b0eFHy-qmLN1IfFToEqDsJBZMo9GFlXY51NH8ndVZLQ",
//!          ...
//! }
//! ```
//!
//! The root node tells us that "a" maps to child node hash "8xB2EYWrEahgCbqVEwu6QdmoTg-FpvQglfe8tyjAk6I".
//!
//! We then load the node with hash "8xB2EYWrEahgCbqVEwu6QdmoTg-FpvQglfe8tyjAk6I".
//! We look for the prefix "b" (the 2nd char of the hash) in that node's map
//!
//! {
//!    "a": "ITrgeX8jQ6-LpeRgeqiFo9U4pCpCc1yl4tPHalp_8Qs",
//!    "b": "Om4qtdX7Dgf56R8qh986Dl0Hs7NTuRp1xgwCLw97jkU",
//!    "c": "RQVDSq4Aelf9M95n3AWi2zKg5nrDWMMXwwnTTnfv62U",
//!         ...
//! }
//!
//! We load the node Om4qtdX7Dgf56R8qh986Dl0Hs7NTuRp1xgwCLw97jkU.
//! The keys at this level are longer (41 chars) and represent the remainder of the 43 character hash
//! length, indicating that these are the leaf node hashes. We look for
//! `cdefghij8fCrf9ppASRQqYAuBqOSCWTflF3tXiIvS` (the remainder of the hash).
//!
//! Fetching the contents of `yc28SGqEJmjLDQVjvnHllStFIKO7S_AKlaCApJVWRYI` gives us the crate metadata
//! we're looking for.
//!
//! {
//!    "1CS82oDySYH3voDtZmAg5tr7t4SF2A0U4YQOrO6r8": "9hgpwbJ5P_xd-yo_YeAN3fg7b1az7PrZTQ69qUqgIsE",
//!    "cdefghij8fCrf9ppASRQqYAuBqOSCWTflF3tXiIvS": "yc28SGqEJmjLDQVjvnHllStFIKO7S_AKlaCApJVWRYI",
//!    "3NCS9N9-_qKRFDF8GRA-JMst0XoWAMHGmKFUCsnHY": "VSH4vWmuUH3h1UQ_dJ5IDVlCEn3baQhQHJEHc6_5YjA",
//!    ...
//! }
//!
//! In this example, I used a tree depth of 2, with 64 branches at each node (1 base64 char prefixes).
//! With this structure, the entire current crates.io index is split into about
//! 4000 index files + 1 file per crate version (as we have today).
//!
pub mod fsstore;
pub mod memstore;
use futures::{
    StreamExt, TryStreamExt,
    lock::Mutex,
    stream::{self},
};
use quick_cache::sync::Cache;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{
    collections::{BTreeMap, HashSet},
    fmt::Display,
    future::Future,
    sync::Arc,
};
use thiserror::Error;
use tracing::{debug, info, trace};
type MResult<T, E> = std::result::Result<T, Error<E>>;

#[derive(Clone)]
pub struct TreeParameters {
    root: Option<Hash>,
    depth: usize,
    bredth: usize,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Hash)]
pub struct Hash(String);

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(transparent)]
struct Key(String);

impl Key {
    pub fn new(name: &str, params: &TreeParameters) -> Self {
        let hash_prefix_len = params.depth * params.bredth;
        let digest = Sha256::digest(name);
        let mut out = String::with_capacity(name.len() + hash_prefix_len);
        out.extend(
            digest
                .iter()
                .flat_map(|byte| (0..8).map(move |bit| ((byte >> bit & 1) + b'0') as char))
                .take(hash_prefix_len),
        );
        out.push_str(name);
        Key(out)
    }

    pub fn split(&self, at: usize, params: &TreeParameters) -> (Key, Key) {
        let (l, r) = self.0.split_at(at * params.bredth);
        (Key(l.to_string()), Key(r.to_string()))
    }

    pub fn take_one(&mut self, params: &TreeParameters) -> Key {
        if self.0.len() < params.bredth {
            let mut r = Key(String::new());
            std::mem::swap(&mut r, self);
            return r;
        }

        let mut r = Key(self.0.split_off(params.bredth));
        std::mem::swap(&mut r, self);
        r
    }
}

#[derive(Debug, Error)]
pub enum Error<T: Display> {
    #[error("hash `{hash}` should be present, but was missing")]
    NotFound { hash: String },
    #[error("failed to decode JSON")]
    Json(serde_json::Error),
    #[error("backend error: {inner:?}")]
    Backend { inner: T },
    #[error("hash mismatch: expected {expected}, actual {actual}")]
    HashMismatch { expected: String, actual: String },
}

impl<T> From<T> for Error<T>
where
    T: Display,
{
    fn from(value: T) -> Self {
        Self::Backend { inner: value }
    }
}

pub trait ReadObjectStore {
    type E: Display;

    /// Read data by hash.
    fn read(
        &self,
        hash: &str,
        is_leaf: bool,
    ) -> impl Future<Output = Result<Option<Vec<u8>>, Self::E>>;
}

pub trait WriteObjectStore: ReadObjectStore {
    /// Write data into the store with the given hash.
    fn write(
        &self,
        hash: &str,
        data: Vec<u8>,
        is_leaf: bool,
    ) -> impl Future<Output = Result<(), Self::E>>;
}

pub trait GCObjectStore: ReadObjectStore {
    /// Delete an object.
    fn delete(&self, hash: &str, is_leaf: bool) -> impl Future<Output = Result<(), Self::E>>;

    /// Enumerate all hashes in the store (regardless of reachability).
    fn enumerate_all(
        &self,
    ) -> impl std::future::Future<Output = Result<HashSet<(String, bool)>, Self::E>>;
}

#[derive(Serialize, Deserialize, Default, Debug, Clone)]
struct Node(BTreeMap<Key, Hash>); // prefix -> node hash

pub struct MerkleStore<B: ReadObjectStore> {
    backend: B,
    params: Mutex<Option<TreeParameters>>,
    node_cache: quick_cache::sync::Cache<Hash, Arc<Node>>,
}

impl<B: ReadObjectStore> MerkleStore<B> {
    pub fn new(backend: B) -> Self {
        Self {
            backend,
            params: Mutex::new(None),
            node_cache: Cache::new(usize::MAX),
        }
    }

    pub fn backend(&self) -> &B {
        &self.backend
    }

    pub async fn is_configured(&self) -> bool {
        self.params.lock().await.is_some()
    }

    pub async fn configure(&self, root: Option<&str>, depth: usize, bredth: usize) {
        *self.params.lock().await = Some(TreeParameters {
            root: root.map(|root| Hash(root.to_string())),
            depth,
            bredth,
        });
    }

    pub async fn get_file(&self, logical_name: &str) -> MResult<Option<Vec<u8>>, B::E> {
        if let Some(hash) = self.get_hash(logical_name).await? {
            Ok(Some(self.read_object(&hash, true).await?))
        } else {
            Ok(None)
        }
    }

    pub async fn get_file_by_hash(&self, hash: &str) -> MResult<Option<Vec<u8>>, B::E> {
        Ok(Some(self.read_object(&Hash(hash.to_string()), true).await?))
    }

    pub async fn get_file_hash(&self, logical_name: &str) -> MResult<Option<String>, B::E> {
        Ok(self.get_hash(logical_name).await?.map(|h| h.0))
    }

    pub async fn root(&self) -> Option<String> {
        self.params().await.root.map(|root| root.0)
    }

    async fn params(&self) -> TreeParameters {
        self.params
            .lock()
            .await
            .clone()
            .expect("must call configure before using the tree")
    }

    // Reads from the store by hash.
    async fn read_object(&self, hash: &Hash, is_leaf: bool) -> MResult<Vec<u8>, B::E> {
        trace!(hash = &hash.0, "write");

        let data = self
            .backend
            .read(&hash.0, is_leaf)
            .await?
            .ok_or_else(|| Error::NotFound {
                hash: hash.0.clone(),
            })?;
        let verification_hash = compute_hash(&data);
        if hash != &verification_hash {
            return Err(Error::HashMismatch {
                expected: hash.0.to_string(),
                actual: verification_hash.0,
            });
        }

        trace!(len = data.len(), hash = hash.0, "reading object");
        Ok(data)
    }

    async fn get_hash(&self, logical_name: &str) -> MResult<Option<Hash>, B::E> {
        let params = self.params().await;
        let key = Key::new(logical_name, &params);
        debug!(logical_name, key = key.0, "get_hash");

        let Some(head) = &params.root else {
            return Ok(None);
        };

        let mut node = self.load_node(head).await?;
        let mut remaining_key = key.clone();

        loop {
            trace!("looking up {}", remaining_key.0);
            if let Some(leaf) = node.0.get(&remaining_key) {
                debug!(logical_name, content_hash = leaf.0, "get_hash resolved");
                return Ok(Some(leaf.clone()));
            }

            let subkey = remaining_key.take_one(&params);

            let child = match node.0.get(&subkey) {
                Some(h) => h,
                None => {
                    return Ok(None);
                }
            };

            node = self.load_node(&child).await?;
        }
    }

    async fn load_node(&self, hash: &Hash) -> MResult<Arc<Node>, B::E> {
        self.node_cache
            .get_or_insert_async(hash, async {
                Ok(Arc::new(self.load_node_uncached(hash).await?))
            })
            .await
    }

    async fn load_node_uncached(&self, hash: &Hash) -> MResult<Node, B::E> {
        let data = self.read_object(hash, false).await?;
        let data = tracing::trace_span!("json-parsing").in_scope(||serde_json::from_slice(&data).map_err(Error::Json))?;
        Ok(data)
    }
}

impl<B: WriteObjectStore> MerkleStore<B> {
    // Saves an object into the store, replacing if the name was already used.
    // Returns the object hash.
    pub async fn put_object(&mut self, logical_name: &str, data: Vec<u8>) -> MResult<Hash, B::E> {
        let mut params = self.params.lock().await;
        let params = &mut *params.as_mut().unwrap();

        let new_root = self.insert(params, logical_name, data).await?;
        params.root = Some(new_root.clone());
        Ok(new_root)
    }

    /// Overwrite the existing tree with the data.
    pub async fn overwrite<F>(
        &mut self,
        d: impl IntoIterator<Item = &String>,
        f: F,
    ) -> MResult<Hash, B::E>
    where
        F: AsyncFn(&String) -> Result<Vec<u8>, B::E>,
    {
        let mut params = self.params.lock().await;
        let params = &mut *params.as_mut().unwrap();

        // insert leaf nodes.
        let mut prev = BTreeMap::new();
        let mut futures = stream::iter(d.into_iter().map(|name| {
            let f = &f;
            let self_shared = &*self;
            let key = Key::new(&name, params);
            async move {
                let object_hash = self_shared.write_object(f(&name).await?, true).await?;
                MResult::Ok((key, object_hash))
            }
        }))
        .buffer_unordered(10);
        while let Some((key, object_hash)) = futures.try_next().await? {
            prev.insert(key, object_hash);
        }
        drop(futures);

        info!("hashed {} leaf nodes", prev.len());

        // Insert metadata nodes.
        for d in (1..params.depth + 1).rev() {
            debug!("inserting nodes at depth = {d}");
            let mut next: BTreeMap<Key, Node> = BTreeMap::new();
            for (key, object_hash) in prev.into_iter() {
                let (bin, key) = key.split(d, &params);
                let entry = next.entry(bin).or_default();
                entry.0.insert(key, object_hash);
            }
            trace!("split into {} bins", next.len());

            // Insert all the nodes
            prev = BTreeMap::new();
            let mut futures = stream::iter(next.into_iter().map(|(key, subtree)| {
                let self_shared = &*self;
                async move {
                    let child_hash = self_shared.write_node(&subtree).await?;
                    MResult::Ok((key, child_hash))
                }
            }))
            .buffer_unordered(10);
            while let Some((key, object_hash)) = futures.try_next().await? {
                prev.insert(key, object_hash);
            }
        }

        let root_hash = self.write_node(&Node(prev)).await?;

        params.root = Some(root_hash.clone());
        Ok(root_hash)
    }

    async fn insert(
        &self,
        params: &mut TreeParameters,
        logical_name: &str,
        data: Vec<u8>,
    ) -> MResult<Hash, B::E> {
        let key = Key::new(logical_name, &params);
        debug!(logical_name, key = key.0, "inserting object");

        // Stack of (node, subkey used to reach this node)
        let mut stack: Vec<(Node, Key)> = Vec::with_capacity(params.depth);

        let root = match &params.root {
            Some(root) => (&*self.load_node(root).await?).clone(),
            None => Node::default(),
        };

        let mut current = root;
        let mut remaining_key = key.clone();
        for _ in 0..params.depth {
            let subkey = remaining_key.take_one(&params);

            let child = match current.0.get(&subkey) {
                Some(h) => (&*self.load_node(h).await?).clone(),
                None => Node::default(),
            };

            stack.push((current, subkey));
            current = child;
        }

        // Write the object data
        let content_hash = self.write_object(data, true).await?;

        // Insert leaf
        debug!(key = key.0, hash = content_hash.0, "writing leaf");
        current.0.insert(remaining_key, content_hash);

        // Walk back up, writing nodes
        while let Some((mut parent, subkey)) = stack.pop() {
            let child_hash = self.write_node(&current).await?;
            parent.0.insert(subkey, child_hash);
            current = parent;
        }

        let new_root_hash = self.write_node(&current).await?;
        debug!(root = new_root_hash.0, "new root hash");
        Ok(new_root_hash)
    }

    // Hashes the data and writes into the store.
    // Returns the object hash.
    async fn write_object(&self, data: Vec<u8>, is_leaf: bool) -> MResult<Hash, B::E> {
        let hash = compute_hash(&data);
        trace!(hash = &hash.0, "write");
        self.backend.write(&hash.0, data, is_leaf).await?;
        Ok(hash)
    }

    async fn write_node(&self, node: &Node) -> MResult<Hash, B::E> {
        let data = serde_json::to_vec(&node).map_err(Error::Json)?;
        self.write_object(data, false).await
    }
}

impl<B: GCObjectStore> MerkleStore<B> {
    // Deletes an object from the store.
    async fn delete_object(&self, hash: &Hash, is_leaf: bool) -> MResult<(), B::E> {
        Ok(self.backend.delete(&hash.0, is_leaf).await?)
    }

    async fn enumerate_live_objects(&self) -> MResult<HashSet<(String, bool)>, B::E> {
        let mut live = HashSet::new();
        let params = self.params().await;
        if let Some(head) = &params.root {
            let mut stack: Vec<(Hash, usize)> = Vec::new();
            stack.push((head.clone(), 0));
            live.insert((head.0.clone(), false));

            while let Some((node_hash, depth)) = stack.pop() {
                let node = self.load_node(&node_hash).await?;
                for hash in node.0.values() {
                    let is_leaf = depth >= params.depth;
                    if live.insert((hash.0.clone(), is_leaf)) && !is_leaf {
                        stack.push((hash.clone(), depth + 1));
                    }
                }
            }
        };

        Ok(live)
    }

    pub async fn gc(&self) -> MResult<(), B::E> {
        debug!("starting GC");

        let live = self.enumerate_live_objects().await?;
        debug!(live = live.len(), "live objects enumerated");

        let all = self.backend.enumerate_all().await?;
        debug!(all = all.len(), "all objects enumerated");

        let dead: Vec<_> = all.difference(&live).cloned().collect();
        debug!(dead = dead.len(), "GC completed");

        for (entry, is_leaf) in &dead {
            trace!(hash = entry, is_leaf, "deleted");
            self.delete_object(&Hash(entry.to_owned()), *is_leaf)
                .await?
        }
        debug!("GC delete completed");

        Ok(())
    }
}

fn compute_hash(data: &[u8]) -> Hash {
    use base64::Engine as _;
    let digest = Sha256::digest(data);
    let algorithm = base64::engine::general_purpose::URL_SAFE_NO_PAD;
    Hash(algorithm.encode(digest))
}

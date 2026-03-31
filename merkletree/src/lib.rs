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
//! ContentHash function: SHA256 + base64-encode (43 chars).
//!
//! Lookup process:
//! 1. Load the root node hash from somewhere (config.json?).
//! 2. Validate that the root node hash using TUF (not defined how that works here).
//! 3. ContentHash the crate name to get the name hash.
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
    StreamExt, TryStreamExt, lock::Mutex, stream::{self, FuturesUnordered}
};
use quick_cache::sync::Cache;
use sha2::{Digest, Sha256};
use std::{
    collections::{HashMap, HashSet},
    fmt::Display,
    future::Future,
    sync::Arc,
};
use thiserror::Error;
use tracing::{debug, trace};

use crate::bitslice::{IndexNode, LeafNode, Node};
type MResult<T, E> = std::result::Result<T, Error<E>>;
mod bitslice;
use bitslice::SplitNameHash;

pub use crate::bitslice::ContentHash;

#[derive(Clone)]
pub struct TreeParameters {
    root: Option<ContentHash>,
    depth: u8,
    bredth: u8,
}

#[derive(Debug, Error)]
pub enum Error<T: Display> {
    #[error("hash `{hash}` should be present, but was missing")]
    NotFound { hash: ContentHash },
    #[error("failed to decode JSON")]
    Json(serde_json::Error),
    #[error("backend error: {inner:?}")]
    Backend { inner: T },
    #[error("hash mismatch: expected {expected}, actual {actual}")]
    HashMismatch {
        expected: ContentHash,
        actual: ContentHash,
    },
}

impl<T> From<T> for Error<T>
where
    T: Display,
{
    fn from(value: T) -> Self {
        Self::Backend { inner: value }
    }
}

pub trait TreeReader {
    type Error: Display;

    /// Read data by hash.
    fn read(
        &self,
        hash: &ContentHash,
        is_leaf: bool,
    ) -> impl Future<Output = Result<Option<Vec<u8>>, Self::Error>>;
}

pub trait TreeWriter: TreeReader {
    /// Write data into the store with the given hash.
    fn write(
        &self,
        hash: &ContentHash,
        data: Vec<u8>,
        is_leaf: bool,
    ) -> impl Future<Output = Result<(), Self::Error>>;
}

pub trait GCObjectStore: TreeReader {
    /// Delete an object.
    fn delete(
        &self,
        hash: &ContentHash,
        is_leaf: bool,
    ) -> impl Future<Output = Result<(), Self::Error>>;

    /// Enumerate all hashes in the store (regardless of reachability).
    fn enumerate_all(
        &self,
    ) -> impl std::future::Future<Output = Result<HashSet<(ContentHash, bool)>, Self::Error>>;
}

pub struct MerkleStore<B: TreeReader> {
    backend: B,
    node_cache: quick_cache::sync::Cache<ContentHash, Arc<Node>>,
    params: Mutex<Option<TreeParameters>>,
    pending_writes: std::sync::Mutex<Vec<(String, ContentHash)>>,
}

impl<B: TreeReader> MerkleStore<B> {
    pub fn new(backend: B) -> Self {
        Self {
            backend,
            params: Mutex::new(None),
            node_cache: Cache::new(usize::MAX),
            pending_writes: std::sync::Mutex::new(Vec::new()),
        }
    }

    pub fn backend(&self) -> &B {
        &self.backend
    }

    pub async fn is_configured(&self) -> bool {
        self.params.lock().await.is_some()
    }

    pub async fn configure(&self, root: Option<ContentHash>, depth: usize, bredth: usize) {
        *self.params.lock().await = Some(TreeParameters {
            root: root,
            depth: depth as u8,
            bredth: bredth as u8,
        });
    }

    pub async fn get_file(&self, name: &str) -> MResult<Option<Vec<u8>>, B::Error> {
        match self.get_file_hash(name).await? {
            Some(hash) => Ok(Some(self.get_file_by_hash(&hash).await?)),
            None => Ok(None),
        }
    }

    pub async fn get_file_by_hash(&self, hash: &ContentHash) -> MResult<Vec<u8>, B::Error> {
        Ok(self.read_object(hash, true).await?)
    }

    pub async fn get_file_hash(&self, name: &str) -> MResult<Option<ContentHash>, B::Error> {
        trace!(?name, "get_file_hash");
        let key = SplitNameHash::new(name, 32);
        let hash = self
            .lookup(self.root().await.as_ref(), key)
            .await?
            .as_ref()
            .and_then(|node| node.as_leaf())
            .and_then(|node| node.get(name))
            .cloned();
        Ok(hash)
    }

    pub async fn root(&self) -> Option<ContentHash> {
        self.params().await.root
    }

    async fn params(&self) -> TreeParameters {
        self.params
            .lock()
            .await
            .clone()
            .expect("must call configure before using the tree")
    }

    // Reads from the store by hash.
    async fn read_object(&self, hash: &ContentHash, is_leaf: bool) -> MResult<Vec<u8>, B::Error> {
        let data = self
            .backend
            .read(&hash, is_leaf)
            .await?
            .ok_or_else(|| Error::NotFound { hash: hash.clone() })?;
        let verification_hash = compute_hash(&data);
        if hash != &verification_hash {
            return Err(Error::HashMismatch {
                expected: hash.clone(),
                actual: verification_hash,
            });
        }
        Ok(data)
    }

    async fn lookup(
        &self,
        mut hash: Option<&ContentHash>,
        mut key: SplitNameHash,
    ) -> MResult<Option<Arc<Node>>, B::Error> {
        trace!(?key, "lookup");
        let mut node = None;
        while let Some(h) = hash {
            node = Some(self.load_node(&h).await?);
            match node.as_deref().unwrap() {
                Node::Index(index_node) => {
                    if key.is_empty() {
                        return Ok(node);
                    }
                    let (remaining, index) = key.split(index_node.len_bits());
                    key = remaining;
                    hash = index_node.get(index);
                }
                Node::Leaf(..) => return Ok(node),
            }
        }
        Ok(node)
    }

    async fn load_node_uncached(&self, hash: &ContentHash) -> MResult<Node, B::Error> {
        let data = self.read_object(hash, false).await?;
        let node = Node::deserialize(data);
        trace!(?hash, "load_node");
        Ok(node)
    }

    async fn load_node(&self, hash: &ContentHash) -> MResult<Arc<Node>, B::Error> {
        self.node_cache
            .get_or_insert_async(hash, async {
                Ok(Arc::new(self.load_node_uncached(hash).await?))
            })
            .await
    }
}

impl<B: TreeWriter> MerkleStore<B> {
    // Saves an object into the store, replacing if the name was already used.
    pub async fn put_object(&self, logical_name: &str, data: Vec<u8>) -> MResult<(), B::Error> {
        self.insert(logical_name, data).await?;
        Ok(())
    }

    async fn insert(&self, name: &str, data: Vec<u8>) -> MResult<(), B::Error> {
        let hash = self.write_object(data, true).await?;
        self.pending_writes
            .lock()
            .unwrap()
            .push((name.to_string(), hash));
        Ok(())
    }

    pub async fn commit(&self) -> MResult<(), B::Error> {
        debug!("commit:starting");
        // Hold the lock to prevent dropped writes.
        let mut params = self.params.lock().await;
        let params = &mut *params.as_mut().expect("no params");
        let root = params.root;
        let tree_depth = params.depth;
        let tree_bredth = params.bredth;

        // Group all the pending writes into buckets by hash.
        let pending = std::mem::take(&mut *self.pending_writes.lock().unwrap());
        debug!(pending = pending.len(), "commit:grouping");
        let mut premap: HashMap<SplitNameHash, Vec<(String, ContentHash)>> = HashMap::new();
        for (name, hash) in pending {
            let key = SplitNameHash::new(&name, tree_depth * tree_bredth);
            premap.entry(key).or_default().push((name, hash))
        }

        debug!(pending = premap.len(), "commit:writing leaves");
        let mut map: HashMap<SplitNameHash, Vec<(SplitNameHash, ContentHash)>> = HashMap::new();
        let mut futures = stream::iter(premap.into_iter().map(|(key, v) | async move {
            // Find an existing leaf node to merge with.
            let node = self.lookup(root.as_ref(), key).await?;
            let mut node = node
                .as_ref()
                .and_then(|node| node.as_leaf())
                .cloned()
                .unwrap_or_else(|| LeafNode::new());
            for (key, value) in v {
                node.insert(key, value);
            }
            let hash = self.write_node(&Node::Leaf(node)).await?;
            MResult::Ok((key, hash))
        })).buffer_unordered(50);
        while let Some((key, hash)) = futures.try_next().await? {
            if tree_depth == 0 {
                params.root = Some(hash);
                return Ok(());
            }
            let (left, right) = key.split(tree_bredth * (tree_depth - 1));
            map.entry(right).or_default().push((left, hash));
        }

        let mut next_root = root;
        for depth in (0..tree_depth).rev() {
            debug!(pending = map.len(), depth, "commit:writing tree at depth");
            let mut next_map: HashMap<SplitNameHash, Vec<_>> = HashMap::new();
            let mut futures = stream::iter(map.into_iter().map(|(key, v)| async move {
                trace!("writing into node {key:?}");
                let node = self.lookup(root.as_ref(), key).await?;
                let mut node = node
                    .as_ref()
                    .and_then(|n| n.as_index())
                    .cloned()
                    .unwrap_or_else(|| IndexNode::new(tree_bredth));

                for (index, value) in v {
                    node.insert(index, value);
                }

                let hash = self.write_node(&Node::Index(node)).await?;
                MResult::Ok((key, hash))
            })).buffer_unordered(50);
            while let Some((key, hash)) = futures.try_next().await? {
                next_root = Some(hash);
                if depth > 0 {
                    let (left, right) = key.split(tree_bredth * (depth - 1));
                    next_map.entry(right).or_default().push((left, hash));
                }
            }

            map = next_map;
        }

        params.root = next_root;
        Ok(())
    }

    /// Hashes the data and writes into the store.
    /// Returns the object hash.
    async fn write_object(&self, data: Vec<u8>, is_leaf: bool) -> MResult<ContentHash, B::Error> {
        let hash = compute_hash(&data);
        trace!(hash = ?hash, "write");
        self.backend.write(&hash, data, is_leaf).await?;
        Ok(hash)
    }

    async fn write_node(&self, node: &Node) -> MResult<ContentHash, B::Error> {
        trace!(?node, "write_node");
        let data = node.serialize();
        self.write_object(data, false).await
    }
}

impl<B: GCObjectStore> MerkleStore<B> {
    // Deletes an object from the store.
    async fn delete_object(&self, hash: &ContentHash, is_leaf: bool) -> MResult<(), B::Error> {
        Ok(self.backend.delete(&hash, is_leaf).await?)
    }

    async fn enumerate_live_objects(&self) -> MResult<HashSet<(ContentHash, bool)>, B::Error> {
        let mut live = HashSet::new();
        let params = self.params().await;

        if let Some(head) = &params.root {
            // Takes the content hash by value
            let load_node = async|hash: ContentHash|self.load_node(&hash).await;
            let mut pending = FuturesUnordered::new();
            // Insert the root to get started
            pending.push(load_node(*head) );
            live.insert((head.clone(), false));
            while let Some(node) = pending.try_next().await? {
                match &*node {
                    Node::Index(index_node) => {
                        for hash in index_node.iter() {
                            if live.insert((hash.clone(), false)) {
                                let hash = hash.clone();
                                pending.push(load_node(hash) );
                            }
                        }
                    }
                    Node::Leaf(leaf_node) => {
                        for (name, hash) in leaf_node.iter() {
                            trace!(name, ?hash);
                            live.insert((hash.clone(), true));
                        }
                    }
                }
            }
        };

        Ok(live)
    }

    pub async fn gc(&self) -> MResult<(), B::Error> {
        debug!("starting GC");

        let all = self.backend.enumerate_all().await?;
        debug!(all = all.len(), "all objects enumerated");

        let live = self.enumerate_live_objects().await?;
        debug!(live = live.len(), "live objects enumerated");

        let dead: Vec<_> = all.difference(&live).cloned().collect();
        debug!(dead = dead.len(), "GC completed");

        for (entry, is_leaf) in &dead {
            trace!(hash = ?entry, is_leaf, "deleted");
            self.delete_object(entry, *is_leaf).await?
        }
        debug!("GC delete completed");

        Ok(())
    }
}

fn compute_hash(data: &[u8]) -> ContentHash {
    let output: [u8; 32] = Sha256::digest(data).into();
    output.into()
}

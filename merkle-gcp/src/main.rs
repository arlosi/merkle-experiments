use std::{path::Path, sync::atomic::AtomicBool, thread, time::Duration};

use futures::{StreamExt, TryStreamExt, stream};
use gix::{Commit, Remote, Repository, open::Options};
use merkletree::{TreeParameters, fscache};
use object_store::{BackoffConfig, ObjectStoreExt, RetryConfig};
use serde::{Deserialize, Serialize};
use tracing_subscriber::{EnvFilter, field::MakeExt as _};

use crate::gcp::GcpStore;
mod gcp;

/*
export GOOGLE_BUCKET=rf-tuf-cargo-merkle-experiments
*/

fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .map_fmt_fields(|f| f.debug_alt())
        .init();

    const PARALLEL: usize = 10;

    let gcp = object_store::gcp::GoogleCloudStorageBuilder::from_env()
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

    let backend = fscache::FsCache::new("cache", GcpStore::new(&gcp))?;
    let store = merkletree::RwMerkleStore::new(
        backend,
        PARALLEL,
        TreeParameters {
            depth: 2,
            breadth: 6,
        },
    );
    let rt = tokio::runtime::Runtime::new().unwrap();
    let config_path = object_store::path::Path::from("config.json");

    let mut config = rt.block_on(async {
        let config: RegistryConfig =
            serde_json::from_slice(&gcp.get(&config_path).await?.bytes().await?)?;
        let merkle = config.merkle.as_ref().unwrap();
        println!("merkle root hash: {}", merkle.root);
        println!("git commit: {}", merkle._corresponding_git_commit);
        store
            .set_root(merkle.root.as_str().try_into().unwrap())
            .await;
        println!("enumerating merkle tree...");
        println!("tree contains {} nodes", store.enumerate().await?.len());
        anyhow::Ok(config)
    })?;

    let mut last_head = config
        .merkle
        .as_ref()
        .unwrap()
        ._corresponding_git_commit
        .clone();

    println!("opening repo...");
    let repo = open_or_clone(
        Path::new("index"),
        "https://github.com/rust-lang/crates.io-index",
    )?;
    let remote = repo
        .find_default_remote(gix::remote::Direction::Fetch)
        .unwrap()?;

    loop {
        println!("updating git repo...");
        update(&remote)?;
        let head_commit = repo
            .find_reference("refs/remotes/origin/master")?
            .peel_to_commit()?;
        let head = head_commit.id.to_string();
        println!("lastest commit is: {head}");

        rt.block_on(async {
            println!("inserting...");

            let changes = changed_files(&repo, &last_head, &head)?;
            if !changes.is_empty() {
                println!("uploading {} changes", changes.len());
                thread::sleep(Duration::from_secs(3));
                let store = &store;
                let mut changes = stream::iter(changes.into_iter().map(|path| {
                    let head_commit = &head_commit;
                    async move {
                        let (_, name) = path.rsplit_once('/').unwrap();
                        if let Some(data) = read(head_commit, &path)? {
                            println!("{name}");
                            anyhow::Ok(store.put_object(&name, data).await?)
                        } else {
                            println!("DELETE: {name}");
                            anyhow::Ok(store.delete(&name).await?)
                        }
                    }
                }))
                .buffer_unordered(PARALLEL);
                while changes.try_next().await?.is_some() {}

                store.commit().await?;
                let new_root = store.root().await.unwrap();
                let x = config.merkle.as_mut().unwrap();
                last_head = head;
                x._corresponding_git_commit = last_head.clone();
                x.root = format!("{new_root}");
                println!("{x:?}");
                gcp.put(&config_path, serde_json::to_vec_pretty(&config)?.into())
                    .await?;
            }
            anyhow::Ok(())
        })?;

        println!("sleeping");
        thread::sleep(Duration::from_secs(30));
    }
}

fn update(remote: &Remote) -> anyhow::Result<()> {
    remote
        .connect(gix::remote::Direction::Fetch)?
        .prepare_fetch(gix::progress::Discard, Default::default())?
        .receive(gix::progress::Discard, &AtomicBool::default())?;
    Ok(())
}

fn open_or_clone(path: &Path, url: &str) -> anyhow::Result<Repository> {
    if path.exists() {
        // Open existing repo
        Ok(gix::open(path)?)
    } else {
        // Clone repo
        let repo = gix::clone::PrepareFetch::new(
            url,
            path,
            gix::create::Kind::Bare,
            gix::create::Options {
                destination_must_be_empty: true,
                fs_capabilities: None,
            },
            Options::default(),
        )?
        .persist();

        Ok(repo)
    }
}

fn changed_files(repo: &Repository, a: &str, b: &str) -> anyhow::Result<Vec<String>> {
    // Resolve commits
    let commit_a = repo.rev_parse_single(a)?.object()?.into_commit();
    let commit_b = repo.rev_parse_single(b)?.object()?.into_commit();

    // Get trees
    let tree_a = commit_a.tree()?;
    let tree_b = commit_b.tree()?;

    let mut changes = Vec::new();

    tree_b
        .changes()?
        .for_each_to_obtain_tree(&tree_a, |change| {
            if change.entry_mode().is_blob() {
                changes.push(change.location().to_string());
            }
            anyhow::Ok(std::ops::ControlFlow::Continue(()))
        })?;

    Ok(changes)
}

fn read(head: &Commit<'_>, path: &str) -> anyhow::Result<Option<Vec<u8>>> {
    let tree = head.tree()?;
    let Some(entry) = tree.lookup_entry(path.split('/'))? else {
        return Ok(None);
    };
    let object = entry.object()?;
    let blob = object.try_into_blob()?;
    let data = blob.data.clone();
    Ok(Some(data))
}

#[derive(Deserialize, Serialize, Debug, Clone)]
#[serde(rename_all = "kebab-case")]
pub struct RegistryConfig {
    pub dl: String,
    pub api: Option<String>,
    pub merkle: Option<MerkleConfig>,
}

#[derive(Deserialize, Serialize, Debug, Clone)]
#[serde(rename_all = "kebab-case")]
pub struct MerkleConfig {
    pub root: String,
    pub _corresponding_git_commit: String,
}

use std::{
    cell::Cell, collections::HashMap, fs, path::{Path, PathBuf}, sync::Arc
};

use anyhow::{Context};
use clap::{Parser, Subcommand};
use futures::{StreamExt, TryStreamExt as _, stream};
use merkletree::{MerkleStore, fsstore::FsStore};
use serde::{Deserialize, Serialize};
use tracing::{info, trace};
use tracing_subscriber::{EnvFilter, field::MakeExt as _};
use walkdir::{DirEntry, WalkDir};

use crate::gcp::GcpStore;
mod gcp;
mod server;

#[derive(Parser)]
struct Args {
    /// Path to the index metadata
    #[arg(long, default_value = "store")]
    path: PathBuf,

    #[arg(long, default_value_t = 2)]
    tree_depth: usize,

    #[arg(long, default_value_t = 6)]
    tree_bredth: usize,

    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Subcommand)]
enum Cmd {
    /// Store a file by content hash
    Put { logical_name: String, path: PathBuf },
    /// Overwrite the based on a crates.io directory tree.
    PutDir { path: PathBuf },
    /// Lookup a logical name and return the content hash
    Get { logical_name: String },
    /// Collect garbage
    Gc,

    /// Start sparse protocol server
    Serve {
        /// Port
        #[arg(short, long, default_value_t = 3443)]
        port: u16,

        /// Key
        #[arg(long, default_value = "key.pem")]
        key: PathBuf,

        /// Cert
        #[arg(long, default_value = "cert.pem")]
        cert: PathBuf,
    },
}

fn is_hidden(entry: &DirEntry) -> bool {
    entry
        .file_name()
        .to_str()
        .map(|s| s.starts_with("."))
        .unwrap_or(false)
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Args::parse();

    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .map_fmt_fields(|f| f.debug_alt())
        .init();
    eprintln!(
        "tree parameters: depth = {}, bredth = {}",
        cli.tree_depth, cli.tree_bredth
    );
    let bin_count = 1 << (cli.tree_depth * cli.tree_bredth);
    eprintln!("crates will be divided across {bin_count} bins",);
    eprintln!(
        "non-leaf bins will contain about {} entries",
        1 << cli.tree_bredth
    );
    eprintln!(
        "leaf bins will index about {} crates (based on about 240k total crates)",
        240000 / bin_count
    );

    trace!("creating backend");
    // let backend = FsStore::new(&cli.path.join("merkle"))?;
    let backend = GcpStore::new()?;
    let mut store = MerkleStore::new(backend);
    let root_path = cli.path.join("config.json");
    let mut config = std::fs::read(&root_path)
        .ok()
        .and_then(|v| serde_json::from_slice(&v).ok())
        .unwrap_or_else(|| RegistryConfig {
            dl: "https://static.crates.io/crates".to_string(),
            api: Some("https://crates.io/".to_string()),
            merkle: None,
        });

    let root = config.merkle.as_ref().map(|m| m.root.as_str());
    trace!(?root, "configure store");
    store.configure(root.map(|v|v.try_into().unwrap()), cli.tree_depth, cli.tree_bredth).await;

    match cli.cmd {
        Cmd::Serve { port, key, cert } => {
            // server::serve(store, port, &cert, &key).await?;
            return Ok(());
        }
        Cmd::Put { logical_name, path } => {
            let data =
                fs::read(&path).with_context(|| format!("failed to read {}", path.display()))?;
            store.put_object(&logical_name, data).await?;
            store.commit().await.unwrap();
        }
        Cmd::PutDir { path } => {
            add_dir(&mut store, &path).await?;
        }
        Cmd::Get { logical_name } => match store.get_file(&logical_name).await? {
            Some(content) => println!("{}", String::from_utf8(content)?),
            None => println!("not found"),
        },
        Cmd::Gc => {
            store.gc().await?;
        }
    }

    if let Some(root) = store.root().await {
        config.merkle = Some(MerkleConfig {
            root: root.to_string(),
        });
        std::fs::write(&root_path, &serde_json::to_vec_pretty(&config)?)?;
    }

    Ok(())
}

async fn add_dir(store: &mut MerkleStore<GcpStore>, path: &Path) -> anyhow::Result<()> {
// async fn add_dir(store: &mut MerkleStore<FsStore>, path: &Path) -> anyhow::Result<()> {
    info!("collecting files");
    let walker = WalkDir::new(path).into_iter();
    let mut files = HashMap::new();
    for entry in walker.filter_entry(|e| !is_hidden(e)) {
        let entry = entry?;
        let path = entry.path();
        if !path.is_file() {
            continue;
        }
        let logical_name = entry.file_name().to_str().unwrap();
        files.insert(logical_name.to_string(), path.to_path_buf());
    }
    let total = files.len();
    info!("overwriting files {total}");

    let complete = Arc::new(Cell::new(0));
    let mut s = stream::iter(files.into_iter().map(|(name, path)|{
        let store = &*store;
        let complete = complete.clone();
        async move {
            complete.update(|v|v+1);
            if complete.get() % 1000 == 0 {
                println!("{}k", complete.get()/1000);
            }
            store
                .put_object(&name, tokio::fs::read(&path).await.unwrap())
                .await
        }
    })).buffer_unordered(10);
    while s.try_next().await?.is_some() { }

    store.commit().await.unwrap();

    Ok(())
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
}

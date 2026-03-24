use std::{
    collections::HashSet,
    fs,
    path::{Path, PathBuf},
};

use anyhow::{Context, Result};
use clap::{Parser, Subcommand};
use futures::{StreamExt as _, stream::FuturesUnordered};
use merkletree::{ObjectStore, Store, fsstore::FsStore, memstore::CacheStore};
use serde::Deserialize;
use tracing::info;
use tracing_subscriber::EnvFilter;
use walkdir::{DirEntry, WalkDir};
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
    Overwrite { path: PathBuf },
    /// Lookup a logical name and return the content hash
    Get { logical_name: String },
    /// Collect garbage
    Gc,
    /// Perform fake dep resolution
    Resolve { name: String },

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

    let backend = FsStore::new("store")?;
    let cache = CacheStore::new(backend);
    let mut store = Store::new(cache, cli.tree_depth, cli.tree_bredth);
    store.load()?;

    match cli.cmd {
        Cmd::Serve { port, key, cert } => {
            server::serve(store, port, &cert, &key).await?;
            return Ok(());
        }

        Cmd::Put { logical_name, path } => {
            let data =
                fs::read(&path).with_context(|| format!("failed to read {}", path.display()))?;
            store.put_object(&logical_name, &data).await?;
        }
        Cmd::Overwrite { path } => {
            overwrite_with(&mut store, &path).await?;
        }
        Cmd::Get { logical_name } => match store.get_file(&logical_name).await? {
            Some(content) => println!("{}", String::from_utf8(content)?),
            None => println!("not found"),
        },
        Cmd::Gc => {
            store.gc().await?;
        }
        Cmd::Resolve { name } => worst_resolver_ever(&store, &name).await,
    }

    store.save()?;

    Ok(())
}

async fn overwrite_with<T: ObjectStore>(
    store: &mut Store<T>,
    path: &Path,
) -> Result<(), anyhow::Error> {
    let walker = WalkDir::new(path).into_iter();
    let mut files = Vec::new();
    for entry in walker.filter_entry(|e| !is_hidden(e)) {
        let entry = entry?;
        let path = entry.path();
        if !path.is_file() {
            continue;
        }
        let logical_name = entry.file_name().to_str().unwrap();
        files.push((logical_name.to_string(), path.to_path_buf()));
    }
    store.overwrite(&files).await?;

    Ok(())
}

#[derive(Deserialize)]
struct Dependency {
    name: String,
    package: Option<String>,
    kind: String,
    optional: bool,
}

#[derive(Deserialize)]
struct Crate {
    deps: Vec<Dependency>,
}

async fn worst_resolver_ever<T: ObjectStore>(store: &Store<T>, name: &str) {
    let mut inflight = FuturesUnordered::new();
    let mut resolved = HashSet::new();

    resolved.insert(name.to_string());
    inflight.push(resolve_one(store, name.to_string()));

    while let Some(deps) = inflight.next().await {
        for dep in deps {
            if resolved.insert(dep.clone()) {
                println!("resolving {}", dep);
                inflight.push(resolve_one(store, dep));
            }
        }
    }
}

// Get name -> dependencies
async fn resolve_one<T: ObjectStore>(store: &Store<T>, name: String) -> Vec<String> {
    info!("resolving {}", name);

    let bytes = store
        .get_file(&name)
        .await
        .expect("store error")
        .expect("file missing");

    let text = String::from_utf8(bytes).expect("invalid utf8");
    let last_line = text.lines().last().expect("empty file");

    let krate: Crate = serde_json::from_str(last_line).expect("invalid json");

    krate
        .deps
        .into_iter()
        .filter(|dep| dep.kind != "dev" && !dep.optional)
        .map(|dep| dep.package.unwrap_or(dep.name))
        .collect()
}

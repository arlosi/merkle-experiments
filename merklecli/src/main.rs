use std::{
    collections::HashMap,
    fs,
    path::{Path, PathBuf},
};

use anyhow::{Context, Result};
use clap::{Parser, Subcommand};
use merkletree::{MerkleStore, fsstore::FsStore};
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

    let backend = FsStore::new(&cli.path)?;
    let mut store = MerkleStore::new(backend);
    let root_path = cli.path.join("index.json");
    let root = std::fs::read_to_string(&root_path).ok();
    store.configure(root.as_deref(), cli.tree_depth, cli.tree_bredth).await;

    match cli.cmd {
        Cmd::Serve { port, key, cert } => {
            server::serve(store, port, &cert, &key, (cli.tree_depth, cli.tree_bredth)).await?;
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
    }

    if let Some(root) = store.root().await {
        std::fs::write(root_path, root)?;
    }

    Ok(())
}

async fn overwrite_with(
    store: &mut MerkleStore<FsStore>,
    path: &Path,
) -> Result<(), anyhow::Error> {
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
    store.overwrite(files.keys(), |p| Ok(std::fs::read(p)?)).await?;

    Ok(())
}

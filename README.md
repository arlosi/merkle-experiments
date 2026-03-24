# Merkle tree experiments
This repo contains my experiments for using merkle trees for the crates.io index.
It's not intended to be production-quality.

It contains a library `merkletree` and a CLI `merklecli`. The CLI can be used
to create a merkle tree of the crates.io index and serve it to Cargo with a
adapter layer that exposes it via the existing sparse protocol.

## Configuration
The tree is configured by `--tree-depth` and `--tree-bredth`. Depth represents the number of uncached requests (other than the root) required to find the hash of the content requested. A depth of 0 puts all the content hashes in one file (the root index). 

The bredth is the number of bits used at each level of the tree. Passing `--tree-bredth 6` means a branching factor of `2^6 = 64` at each node.

Since crates.io has about 240k crates, a few reasonable combinations are:
- Depth = 1, Bredth = 9: 512 leafs, 476 crates / leaf, 512 items per intermedate node
- Depth = 2, Bredth = 6: 4096 leafs, 59 crates / leaf, 64 items / intermediate node
- Depth = 3, Bredth = 5: 32768 leafs, 8 crates / leaf, 32 items / intermediate node

In general, lower depth means fewer round trips; higher leaf count means less data transferred.

Currently depth/bredth numbers must be known by the client. Of course, for actual implementation they will be stored in the index so when crates.io grows, we can change the tree shape. I currently like the middle option (which is the default if you don't pass the CLI options).

## Examples

### Store files in the merkle tree
This stores the `.gitignore` file in the merkle tree under the name "key" and then retrives it.
```bash
RUST_LOG=merkletree=trace cargo run --release -- --path ./store --tree-depth 2 --tree-bredth 6 put "key" ./.gitignore
RUST_LOG=merkletree=trace cargo run --release -- --path ./store --tree-depth 2 --tree-bredth 6 get "key"
```

### Build the merkle tree for crates.io
Assuming you have a clone of the [crates.io-index git repo](https://github.com/rust-lang/crates.io-index) at `../crates.io-index`. This will create a merkle tree at `./store` in this repo containing all the data from the crates.io index (at the current checkout). If an existing merkle tree is present, the data in it will not be reachable anymore.

```bash
RUST_LOG=merkletree=debug cargo run --release -- --path ./store --tree-depth 2 --tree-bredth 6 overwrite ../crates.io-index
```

### Serve the merkle tree for crates.io
```bash
RUST_LOG=merkletree=debug cargo run --release -- --path ./store --tree-depth 2 --tree-bredth 6 serve
```

## Why are there TLS keys in the repo?
They are self-signed so the server can use HTTP2 (which pretty much requires TLS).
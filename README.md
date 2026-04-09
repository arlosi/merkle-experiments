# Merkle tree experiments
This repo contains my experiments for using a merkle tree for the crates.io index.
It's not intended to be production-quality.

It contains a library `merkletree` and a CLI `merklecli`. The CLI can be used
to create a merkle tree of the crates.io index and serve it to Cargo with a
adapter layer that exposes it via the existing sparse protocol.

## Configuration
When creating the tree, it is configured by `--tree-depth` and `--tree-breadth`. Depth represents the number of uncached requests (excluding the root node) required to find the hash of the content requested. A depth of 0 puts all the content hashes in one file (the root).  The breadth is the number of bits used at each level of the tree. Passing `--tree-breadth 6` means each index node will have `2^6 = 64` entries.

Since crates.io has about 240k crates, a few reasonable combinations are:
- Depth = 1, breadth = 9: 512 leafs, 476 crates / leaf, 512 items per intermedate node
- Depth = 2, breadth = 6: 4096 leafs, 59 crates / leaf, 64 items / intermediate node
- Depth = 3, breadth = 5: 32768 leafs, 8 crates / leaf, 32 items / intermediate node

In general, lower depth means fewer round trips; higher leaf count means less data transferred.


## Lookup procedure
1. Take the sha256 hash of the crate name interpreted as a little-endian integer `H`.
2. Load the root node of the index as `N`.

3. Check the type of the node `N`.
3a. If `N` is an leaf node:
 - Perform binary search on the sorted list, looking for the crate name.
 - Return corresponding the value if it exists.
3b. If `N` is an index node:
 - Count the items to determine the corresponding power of 2 `P`. If it is not a power of 2, error.
 - Index into the items at the position represented by the lower `P` bits of `H`
 - Right shift `H` by `P` bits.

## Examples

### Store files in the merkle tree
This stores the `.gitignore` file in the merkle tree under the name "key" and then retrives it.
```bash
RUST_LOG=merkletree=trace cargo run --release -- --path ./store --tree-depth 2 --tree-breadth 6 put "key" ./.gitignore
RUST_LOG=merkletree=trace cargo run --release -- --path ./store --tree-depth 2 --tree-breadth 6 get "key"
```

### Build the merkle tree for crates.io
Assuming you have a clone of the [crates.io-index git repo](https://github.com/rust-lang/crates.io-index) at `../crates.io-index`. This will create a merkle tree at `./store` in this repo containing all the data from the crates.io index (at the current checkout). If an existing merkle tree is present, the data in it will not be reachable anymore.

```bash
RUST_LOG=merkletree=debug cargo run --release -- --path ./store --tree-depth 2 --tree-breadth 6 overwrite ../crates.io-index
```

### Serve the merkle tree for crates.io
```bash
RUST_LOG=merkletree=debug cargo run --release -- --path ./store --tree-depth 2 --tree-breadth 6 serve
```

## Why are there TLS keys in the repo?
They are self-signed so the server can use HTTP2 (which pretty much requires TLS).
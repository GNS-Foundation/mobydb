# 🐋 MobyDB

**The Geospatial-Native Database — Built on GEP Protocol**

> *Geography is not a column. It is the address.*
> *Time is not a timestamp. It is the epoch.*
> *Identity is not a username. It is the key.*

---

## What is MobyDB?

MobyDB is a geospatial-native database where the primary key is a
three-dimensional spacetime identity address:

```
(H3 cell, GEP Epoch, Ed25519 public key)
 WHERE     WHEN       WHO
```

Every record is cryptographically signed at the source, addressed by
sub-kilometre hexagonal cell, and anchored to an immutable epoch chain.

## Quick Start

```bash
# Build
cargo build --release

# Start server
./target/release/mobydbd serve --data ./mobydata --port 7474

# Write a signed record
curl -X POST http://localhost:7474/write \
  -H "Content-Type: application/json" \
  -d '{ ... }'

# Proximity query: all records within 2 rings of a cell in epoch 9
curl "http://localhost:7474/near/861e8050fffffff?rings=2&epoch=9"

# Seal an epoch
curl -X POST http://localhost:7474/epoch/9/seal

# Generate Merkle proof
curl "http://localhost:7474/proof/861e8050fffffff/9/<pubkey_hex>"
```

## Architecture

```
mobydbd (binary)
├── mobydb-core    — types: SpacetimeAddress, MobyRecord, CompositeKey
├── mobydb-storage — RocksDB engine, composite key, range scans
├── mobydb-merkle  — epoch sealing, Merkle tree, proof generation
├── mobydb-query   — MobyQL: near(), during(), zoom_out()
└── mobydb-server  — axum HTTP API
```

## The Composite Key

48 bytes. Lexicographically sortable. IS the primary index.

```
[h3_cell: 8 bytes][epoch: 8 bytes][pubkey: 32 bytes]
```

RocksDB sorts keys as raw bytes. Big-endian encoding means cells are
physically colocated on disk. `near()` = integer range scans on sorted bytes.
No GiST index. No geometry calculation.

## MobyQL

```
near(cell, rings=2)
  .during(epoch_start=8, epoch_end=12)
  .with_tier(Navigator)
  .limit(100)
  .execute(&store)
```

## GEP Genesis

```
26acb5d998b63d54f2ed92851c5c565db9fe0930fc06b06091d05c0ce4ff8289
```

## License

Apache 2.0 — © GNS Foundation (mobydb.com)

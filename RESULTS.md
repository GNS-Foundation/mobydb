# 🐋 MobyDB vs PostGIS — Benchmark Results

**Date:** April 7, 2026
**Hardware:** Apple M4 Pro, 48GB unified memory, NVMe SSD
**Software:** MobyDB 0.1.0 (Rust, release + LTO) · PostgreSQL 16 + PostGIS 3.5
**Methodology:** 10 iterations per query, first discarded (warmup), median reported

PostGIS tuned with best-practice settings: `shared_buffers=4GB`, `effective_cache_size=12GB`, `work_mem=256MB`, `max_parallel_workers_per_gather=4`, `random_page_cost=1.1`. GiST index on geometry, BRIN on timestamp, B-tree on sensor_id. `VACUUM ANALYZE` after every import.

MobyDB compiled with `opt-level=3`, `lto=true`, `codegen-units=1`. Embedded RocksDB — direct access, no network layer.

---

## Headline Numbers (10M records)

| Query | MobyDB | PostGIS | Speedup |
|---|---|---|---|
| **Q1: Proximity (rings=2)** | 0.24 ms | 8.49 ms | **36×** |
| **Q2: Proximity + Temporal** | 6.40 ms | 15.87 ms | **2.5×** |
| **Q4: Point lookup (rings=0)** | 0.01 ms | 6.88 ms | **846×** |
| **Q5: Write 10K records** | 14.62 ms | 601.28 ms | **41×** |
| **Q5: Write throughput** | 684K rec/s | 16.6K rec/s | **41×** |
| **Q6: Merkle proof gen** | <0.01 ms | N/A | ∞ |

> **MobyDB write throughput includes Ed25519 signature verification on every record. PostGIS writes are unsigned.**

---

## Dataset

Synthetic IoT sensor telemetry distributed across Italy (lat 36.6–47.1, lng 6.6–18.5). Each sensor has its own Ed25519 keypair. Readings assigned to GEP epochs at H3 Resolution 7.

| Scale | Sensors | Epochs | Records | Generation time | MobyDB write rate |
|---|---|---|---|---|---|
| S (100K) | 100 | 1,000 | 101,000 | 2.86s | 35,365 rec/s |
| M (1M) | 1,000 | 1,000 | 1,000,000 | 28.46s | 35,135 rec/s |
| L (10M) | 1,000 | 10,000 | 10,000,000 | 344.98s | 28,987 rec/s |

Reference point: Palermo (38.1157°N, 13.3615°E), H3 cell `871e9a0ecffffff`.

---

## Q1: Proximity Search — `near(cell, rings=2)` vs `ST_DWithin(5km)`

Find all sensor readings within ~5km of Palermo at epoch 50.

| Scale | MobyDB median | PostGIS median | Speedup | MobyDB rows | PostGIS rows |
|---|---|---|---|---|---|
| 100K | 0.03 ms | 0.40 ms | 12× | 9 | 5 |
| 1M | 0.28 ms | 1.52 ms | 5.5× | 135 | 88 |
| **10M** | **0.24 ms** | **8.49 ms** | **36×** | 136 | 91 |

MobyDB query time stays nearly flat as dataset grows. PostGIS degrades as the GiST index grows.

Row count difference: H3 hexagonal rings and ST_DWithin circular radius cover slightly different geometric areas. Both are correct for their respective spatial models.

---

## Q2: Proximity + Temporal — `near(2).during(40,60)` vs `ST_DWithin + epoch BETWEEN`

Combined spatial and temporal filter. MobyDB handles both in one composite key range scan. PostGIS requires two separate index scans.

| Scale | MobyDB median | PostGIS median | Speedup | MobyDB rows | PostGIS rows |
|---|---|---|---|---|---|
| 1M | 6.09 ms | 15.04 ms | 2.5× | 2,835 | 1,848 |
| **10M** | **6.40 ms** | **15.87 ms** | **2.5×** | 2,856 | 1,911 |

---

## Q3: Spatial Aggregation — `zoom_out(5)` vs `GROUP BY + ST_Within`

Aggregate sensor readings from Res-7 (building level) to Res-5 (district level).

| Scale | MobyDB median | PostGIS median | Speedup | MobyDB groups | PostGIS groups |
|---|---|---|---|---|---|
| 1M | 1.21 ms | 0.33 ms | 0.27× | 4 | 21 |
| **10M** | **1.28 ms** | **0.32 ms** | **0.25×** | 4 | 21 |

PostGIS wins here. Group count difference indicates the two queries are not yet fully equivalent — the PostGIS query uses a wider source area. To be investigated and corrected.

---

## Q4: Multi-Ring Scaling — How each engine scales with proximity radius

All results at 10M records, epoch 50.

| Rings | Approx radius | MobyDB | PostGIS | Speedup | Rows (M / P) |
|---|---|---|---|---|---|
| 0 | ~1.3 km | 0.01 ms | 6.88 ms | **846×** | 4 / 4 |
| 1 | ~3.2 km | 0.31 ms | 6.83 ms | **22×** | 52 / 36 |
| 2 | ~5.5 km | 0.81 ms | 6.92 ms | **8.6×** | 136 / 115 |
| 3 | ~7.7 km | 1.34 ms | 7.29 ms | **5.4×** | 222 / 201 |
| 4 | ~10 km | 0.94 ms | 6.96 ms | **7.4×** | 294 / 290 |
| 5 | ~12 km | 1.81 ms | 7.27 ms | **4.0×** | 300 / 300 |

At 10M, **MobyDB wins at every ring distance**. At 1M, PostGIS overtook MobyDB above rings=3. At 10M, MobyDB maintains its lead even at the widest radius. PostGIS query time is dominated by GiST index traversal, which grows with dataset size. MobyDB's composite key scan cost depends only on result set size.

---

## Q5: Write Throughput — 10K record batch

MobyDB write includes Ed25519 signature verification on every record. PostGIS write is unsigned INSERT with geometry construction.

| Scale | MobyDB | PostGIS | Speedup | MobyDB rec/s | PostGIS rec/s |
|---|---|---|---|---|---|
| 100K | 12.51 ms | 280.81 ms | 22× | 799K | 35.6K |
| 1M | 13.17 ms | 144.00 ms | 10× | 759K | 69.4K |
| **10M** | **14.62 ms** | **601.28 ms** | **41×** | **684K** | **16.6K** |

MobyDB write time barely changes with dataset size. PostGIS write time grows dramatically — GiST index maintenance becomes the bottleneck at 10M.

---

## Q6: Merkle Proof Generation (MobyDB exclusive)

Generate and verify an offline-verifiable Merkle proof for a single record. No PostGIS equivalent exists.

| Operation | Time |
|---|---|
| Epoch seal (100 records) | 39–54 ms |
| Proof generation | <0.01 ms |
| Proof verification | <0.01 ms |

The proof bundle can be presented to a NIS2 auditor, an EU AI Act inspector, or a court. It proves: this data existed, at this location, at this time, signed by this identity. No server required.

---

## Scaling Curves — The Key Insight

```
Q1 Proximity (rings=2) across scales:

MobyDB:  100K → 0.03ms    1M → 0.28ms    10M → 0.24ms   (flat)
PostGIS: 100K → 0.40ms    1M → 1.52ms    10M → 8.49ms   (degrading)

Q5 Write across scales:

MobyDB:  100K → 12.51ms   1M → 13.17ms   10M → 14.62ms  (flat)
PostGIS: 100K → 280.81ms  1M → 144.00ms  10M → 601.28ms (volatile)
```

MobyDB performance is nearly constant regardless of dataset size. This is a direct consequence of the composite key design: proximity queries are range scans on sorted bytes, not index lookups. The cost depends on the result set size, not the total dataset size.

---

## Fairness Notes

- Both engines run on the same hardware, same session, same data.
- PostGIS is tuned with production best-practice settings, not defaults.
- PostGIS indexes are optimal: GiST on geometry, BRIN on timestamp, B-tree on sensor_id.
- MobyDB is an embedded engine (like SQLite). PostGIS is client/server. Both are benchmarked in their natural access pattern.
- Q3 results favor PostGIS and are published honestly. The group count mismatch will be corrected.
- Raw JSON results are available in `results/benchmark_results.json`.

---

## Reproduce

```bash
git clone https://github.com/GNS-Foundation/mobydb
cd mobydb
cargo build --release

# Generate 10M records
cargo run --release -p mobydb-tools --bin generate_iot -- \
  --sensors 1000 --epochs 10000 \
  --mobydb-path ./benchmark_data_10m \
  --postgis-csv ./data/iot_10m.csv

# Import to PostGIS (PostgreSQL 16 + PostGIS 3.5)
psql -d benchmark_db -c "\COPY sensor_readings(...) FROM './data/iot_10m.csv' CSV HEADER;"
psql -d benchmark_db -c "UPDATE sensor_readings SET geom = ST_SetSRID(ST_MakePoint(lng, lat), 4326);"
psql -d benchmark_db -c "VACUUM ANALYZE sensor_readings;"

# Run benchmark
cargo run --release -p mobydb-tools --bin run_benchmark -- \
  --scale l --query all --iterations 10 \
  --postgis-url "postgresql://your_user@localhost/benchmark_db" \
  --mobydb-path ./benchmark_data_10m
```

---

## Stack

MobyDB: Rust · RocksDB · h3o (pure Rust H3) · ed25519-dalek · blake3 · Apache 2.0

**GitHub:** https://github.com/GNS-Foundation/mobydb
**Website:** https://mobydb.com
**Benchmark spec:** https://github.com/GNS-Foundation/mobydb-benchmark

---

*Identity = Public Key · Territory = H3 Cell · Accountability = Delegation Chain*

GNS Foundation · ULISSY s.r.l. · mobydb.com

# Measured: 13 storage backends under precog's group-commit append workload

## Setup (measured on this project's VM, 2026-11-17)
- Hardware: 4 vCPU Intel Xeon @ 2.1 GHz VM, 15 GB RAM, ext4 on virtio block device (`/dev/vda`). fdatasync 4 KiB p50 ~180 µs, 27 KB append+fdatasync p50 ~297 µs; preallocated+zeroed file p50 ~123 µs (earlier measurement in this project).
- Harness: `crates/storage-bench` in the precog repo (source: crates/storage-bench/src/main.rs, runner scripts/storage_suite.sh, raw data results/storage.jsonl, table results/STORAGE.md).
- Identical front end for every backend, modelled on precog's WAL: P producer threads each append one value (a "batch" of 27,000 B or 2,700 B, key = (log=producer, seq)) and block until durable; ONE writer thread drains every queued append into one `append_group()` call that returns only when the whole group is on stable storage (fdatasync / O_DSYNC / engine sync commit). Only the backend varies.
- Grid: 13 backends x value size {27 KB, 2.7 KB} x producers {1, 4, 16} x 2 repetitions x 8 s; medians reported. 156 runs.
- Metrics: appends/s, MB/s, average group size, ack latency p50/p99, process CPU µs per append, device write amplification (bytes written to /dev/vda per user byte, from /sys/block/vda/stat after sync; includes journal, metadata and compaction), disk-space amplification, random point gets/s (page cache warm, 20k gets), full scan MB/s (touches every page), reopen time with caches dropped (`echo 3 > /proc/sys/vm/drop_caches`) until reads can be served (for raw logs this includes rebuilding the index by replaying segments).
- Backends and exact configuration:
  - raw-append: O_APPEND file, write + fdatasync (precog today)
  - raw-prealloc: pwrite into recycled pre-written 1 GiB segments + fdatasync
  - raw-falloc: pwrite into `fallocate()`d (unwritten-extent) 1 GiB segments + fdatasync
  - raw-odirect: O_DIRECT|O_DSYNC pwrite into recycled segments, 4 KiB aligned, rewriting the partial tail block each group
  - raw-uring: io_uring (io-uring 0.7.16) linked WRITE + FSYNC(DATASYNC) into recycled segments
  - raw records carry a run generation number; replay stops at a generation mismatch (needed for recycled segments)
  - raft-engine 0.4.2 (crates.io), LogBatch per group, write(sync=true), compression disabled
  - rocksdb (rust-rocksdb 0.25.0 / RocksDB 11.8.1), WriteBatch + sync=true, no compression, 64 MB memtable; rocksdb-blob = same + enable_blob_files, min_blob_size 1 KiB
  - fjall 3.1.13, batch commit + persist(SyncData), 64 MB memtable; fjall-kvsep = KvSeparationOptions::default()
  - redb 4.6.0, one write txn per group, Durability::Immediate
  - lmdb via heed 0.23.1, one write txn per group (default sync)
  - sqlite via rusqlite 0.43.0 (bundled), WAL mode, synchronous=FULL, one txn per group, WITHOUT ROWID table

## Results: median of 2 repetitions (full tables in results/STORAGE.md)

27 KB batches, 16 producers (bandwidth-bound regime):
| backend | MB/s | vs raw-append | ack p99 µs | CPU µs/append | device write amp | point gets/s | scan MB/s | reopen ms (cold) |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| raw-append | 282.0 | 1.00x | 3,512 | 58.2 | 1.04 | 161,770 | 6,055 | 4,812 |
| raw-prealloc | 304.7 | 1.08x | 3,165 | 55.4 | 1.02 | 138,184 | 5,796 | 4,729 |
| raw-falloc | 256.6 | 0.91x | 4,193 | 62.7 | 1.05 | 154,965 | 5,843 | invalid (harness truncated on reopen; fixed) |
| raw-odirect | 305.1 | 1.08x | 3,011 | 49.6 | 1.02 | 17,911 | 1,702 | 5,767 |
| raw-uring | 300.2 | 1.06x | 3,041 | 61.8 | 1.02 | 187,449 | 7,168 | 4,542 |
| raft-engine | 271.0 | 0.96x | 4,237 | 56.2 | 1.03 | 10,350 | 308 | 252 |
| rocksdb | 130.5 | 0.46x | 57,935 | 120.0 | 3.25 | 75,485 | 2,812 | 134 |
| rocksdb-blob | 169.6 | 0.60x | 33,439 | 119.3 | 2.00 | 35,467 | 1,069 | 234 |
| fjall | 109.9 | 0.39x | 74,143 | 139.5 | 4.25 | 92,431 | 3,760 | 75 |
| fjall-kvsep | 183.1 | 0.65x | 32,231 | 87.2 | 2.03 | 63,403 | 3,284 | 25 |
| redb | 140.9 | 0.50x | 6,565 | 114.6 | 1.57 | 52,246 | 1,448 | 2 |
| lmdb | 198.9 | 0.71x | 4,377 | 61.4 | 1.36 | 1,283,251 | 25,262 | 1 |
| sqlite | 91.3 | 0.32x | 63,359 | 196.9 | 2.63 | 13,368 | 4,244 | 2 |

27 KB batches, 1 producer (latency-bound, every append is its own sync):
| backend | appends/s | vs raw-append | ack p50 µs | ack p99 µs |
|---|---:|---:|---:|---:|
| raw-append | 2,726 | 1.00x | 341 | 738 |
| raw-prealloc | 3,623 | 1.33x | 256 | 454 |
| raw-falloc | 2,507 | 0.92x | 368 | 876 |
| raw-odirect | 4,647 | 1.70x | 192 | 426 |
| raw-uring | 3,175 | 1.16x | 286 | 688 |
| raft-engine | 3,107 | 1.14x | 293 | 634 |
| rocksdb | 2,529 | 0.93x | 326 | 708 |
| fjall | 2,697 | 0.99x | 309 | 620 |
| redb | 1,661 | 0.61x | 571 | 1,086 |
| lmdb | 1,825 | 0.67x | 512 | 1,164 |
| sqlite | 2,394 | 0.88x | 324 | 798 |

27 KB, 4 producers: raw-odirect 1.61x, raw-prealloc 1.25x, raft-engine 1.24x, raw-uring 1.20x, raw-falloc 1.02x; rocksdb 0.66x (p99 3.4 ms), fjall 0.68x (p99 3.3 ms), fjall-kvsep 0.89x, lmdb 0.67x, redb 0.56x, sqlite 0.58x.

2.7 KB batches, 16 producers (appends/s, vs raw-append, device write amp): raw-append 29,527 1.00x 1.32; raw-prealloc 35,953 1.22x 1.14; raw-falloc 29,387 1.00x 1.37; raw-odirect 39,153 1.33x 1.13; raw-uring 32,515 1.10x 1.13; raft-engine 32,513 1.10x 1.23; rocksdb 24,587 0.83x 3.07; rocksdb-blob 0.89x 2.27; fjall 0.85x 2.79; fjall-kvsep 0.90x 2.25; redb 9,149 0.31x 6.68; lmdb 12,937 0.44x 4.64; sqlite 12,101 0.41x 7.97.
2.7 KB, 1 producer: device write amp redb 16.0, lmdb 10.8, sqlite 10.2 vs raw-prealloc 2.6 and raw-append 4.0 (each tiny group rewrites at least one 4 KiB page plus metadata).

## Findings (from the measurements above)
1. Preallocated/recycled segments are the cheapest real win: +8% to +33% throughput and lower p99 vs today's O_APPEND, same code complexity. fallocate() alone is NOT a substitute: unwritten extents make it equal to or slower than append (0.83x-1.02x) because each first write converts extents (metadata journal commit).
2. O_DIRECT|O_DSYNC is fastest when latency-bound (1.5x-1.7x at 1-4 producers; p50 192 µs vs 341 µs at 27 KB/1 producer) and lowest CPU, but: (a) converges with buffered prealloc once groups are large (1.08x at 16 producers/27 KB, device-bandwidth bound ~300 MB/s); (b) reads are uncached: point gets 18-36k/s vs 140-700k/s for buffered backends, scans ~1.7 GB/s vs ~6 GB/s — it needs its own cache (as Redpanda/TigerBeetle do); (c) partial-tail-block rewrite adds write amp for small groups (2.58x at 2.7 KB/1 producer, same as other raw variants due to 4 KiB granularity).
3. io_uring linked write+fsync gave no benefit over blocking pwrite+fdatasync for a single-writer WAL (0.89x-1.20x, higher CPU at low concurrency). The sync latency dominates; io_uring's advantage (many in-flight ops, fewer syscalls) does not apply to one writer issuing one sync at a time.
4. raft-engine (TiKV) matches raw-prealloc on writes (0.89x-1.24x of raw-append, lowest CPU, write amp 1.03-1.2) and reopens fast (66-640 ms, index rebuilt from its own checkpointing), but its read path was weak under the 16-producer 27 KB load (10k point gets/s, 308 MB/s scan) and its last crates.io release is from 2024 (git master needed for the 2026 sync() fix, per the embedded_engines notes).
5. LSM engines (RocksDB, fjall) are competitive for small values (0.8x-0.9x at 2.7 KB) but degrade badly for 27 KB values under load: 0.39x-0.46x with p99 of 58-74 ms (memtable flush/compaction write stalls) and 3.3-4.3x device write amplification. Key-value separation (blob files) halves write amp (~2.0x) and improves throughput to 0.60x-0.65x, p99 still 32-33 ms.
6. B-tree engines (redb, LMDB, SQLite) are the wrong place for batch data (0.31x-0.71x, write amp up to 16x for small groups) but excel at what an index store needs: LMDB 1.1-2.4M point gets/s and 18-32 GB/s scans (mmap), redb 0.45-0.67M gets/s, and ~1-2 ms reopen with no replay.
7. Recovery is the raw log's weakness today: rebuilding the in-memory index by replaying every segment took 1.4-5.8 s with cold caches for 0.6-2.4 GB of data (~400-500 MB/s replay). Engines with persistent indexes reopen in 1-250 ms. precog needs per-segment sparse index files plus a checkpoint so replay covers only the active segment (Kafka/Iggy pattern).

## Caveats
- Single VM, single virtio disk, shared CPUs; results reflect a device where fdatasync costs ~120-300 µs. On enterprise NVMe with power-loss protection, flush costs a few µs (see raw_io_and_durability notes), which shifts the bottleneck to CPU/metadata and would likely widen the gap in favour of O_DIRECT and lean raw paths.
- 8-second runs: long-running LSM compaction debt and B-tree growth beyond ~2 GB are not captured; retention/deletion cost was not benchmarked.
- Engines were run with default-ish settings plus compression off; tuning (e.g. RocksDB FIFO compaction, larger memtables, fjall journal settings) could improve their numbers.
- The raw backends' point gets use an in-memory hash index (precog's current design), which flatters their read numbers compared to an on-disk sparse index.

# Rust embedded storage engines as candidates for precog's log store / index store (as of 2026-11-17)

Method note: GitHub API access was blocked in this session (HTTP 403 from the session proxy), so release and recency data comes mostly from the crates.io API (queried 2026-11-17) and fetched GitHub HTML pages. Stars, issue counts and per-repo last-commit dates are missing for most repos; see Gaps.

## Q1. Which engines are appropriate for 3-30 KB immutable values appended at hundreds of MB/s with fsync per group, and what write amplification do they impose?

### Takeaway
Only purpose-built logs (raft-engine; a custom segment log) and LSMs with key-value separation (RocksDB BlobDB, fjall 3 blob files, surrealkv vlog) avoid rewriting 3-30 KB values repeatedly. A plain LSM or a copy-on-write B-tree (redb, LMDB) rewrites or copies large values. raft-engine is the closest match: it is Bitcask-like, with leader-based group commit and `fdatasync` per synced batch. Its crate release is stale (0.4.2, April 2024), although the git master is still maintained.

### Cited Findings
**raft-engine (TiKV)**
- Design: log-structured, Bitcask-like embedded store. Each Raft group has a memtable holding keys and the file locations of its log entries. All groups share one log stream, and the active file rotates past a size threshold — [raft-engine README](https://github.com/tikv/raft-engine)
- Group commit: concurrent writers queue up and the first becomes leader, writing the whole group to the log file. If any write in the group is sync, the leader calls `fdatasync()`. Log batches are atomic — [raft-engine README](https://github.com/tikv/raft-engine)
- Drops Bitcask's hint file to avoid extra background write I/O — [WebSearch summary of PingCAP write-up](https://pingcap.com/?p=9415)
- Vendor numbers vs RocksDB as the Raft log store: QPS up to +4%, tail latency -20%, write I/O -25% to -40%, CPU -12% under heavy load — [PingCAP write-up (via search snippet)](https://pingcap.com/?p=9415). TiKV 6.1 notes report "up to 40%" less write I/O and about 10% less CPU, and say raft-engine is the default log store since v6.1 — [TiKV 6.1 What's New](https://tikv.org/docs/6.1/new-features/overview/); [AskTUG GA thread](https://ask.pingcap.com/t/v6-1-raft-engine-storage-engine-ga/8632)
- Durability fix in 2026: "engine: have sync() actually fsync the active log (#395)" (May 7, 2026). Before this fix an explicit `sync()` may not have fsynced the active file, so anyone depending on a 2024 crate release should check — [raft-engine commits](https://github.com/tikv/raft-engine/commits/master)

**RocksDB (via rust-rocksdb)**
- Integrated BlobDB stores values at or above `min_blob_size` in blob files, with only pointers in the LSM. This avoids copying values "over and over again during compaction", reduces write amplification and eases write stalls — [RocksDB BlobDB wiki](https://github.com/facebook/rocksdb/wiki/BlobDB)
- Blob GC is integrated with compaction. `blob_garbage_collection_age_cutoff` defaults to 0.25 (the oldest 25% of blob files are eligible for relocation). The wiki recommends leveled compaction with BlobDB — [BlobDB wiki](https://github.com/facebook/rocksdb/wiki/BlobDB)
- Blob files are built by flush and compaction and tracked in the MANIFEST. The WAL and its recovery modes are supported, with the same consistency guarantees as plain RocksDB — [BlobDB wiki](https://github.com/facebook/rocksdb/wiki/BlobDB)
- FIFO compaction has table-data write amplification of 1 (plus the WAL). The optional `allow_compaction` mode can roughly double write I/O — [RocksDB FIFO wiki](https://github.com/facebook/rocksdb/wiki/FIFO-compaction-style)

**fjall (pure-Rust LSM)**
- fjall is an LSM "similar to RocksDB". Each keyspace (column family) is a separate LSM tree, with cross-keyspace atomic writes — [fjall README](https://github.com/fjall-rs/fjall)
- KV separation in fjall 3: large values go to immutable sorted blob files (similar to BlobDB/Titan). GC tracks fragmentation and relocates live blobs during compaction, replacing the old manual stop-the-world GC. "Now, a scheme more similar to RocksDB's BlobDB is used" — [Releasing Fjall 3.0, 2026-01-02](https://fjall-rs.github.io/post/fjall-3/)
- Large values are compressed in the journal by default, which can be disabled. Block and blob loads verify a 128-bit xxh3 checksum. The database takes an exclusive file lock — [Fjall 3.0 post](https://fjall-rs.github.io/post/fjall-3/)
- Durability: by default writes go to OS buffers, not disk ("matches RocksDB's default durability"). `Database::persist(PersistMode::SyncAll)` syncs the journal — [fjall README](https://github.com/fjall-rs/fjall); [docs.rs fjall 3.1.13](https://docs.rs/fjall/latest/fjall/)
- Limits: keys up to 65,536 bytes and values up to 2^32 bytes — [docs.rs fjall](https://docs.rs/fjall/latest/fjall/)
- Transactions come in two forms: `SingleWriterTxDatabase` and `OptimisticTxDatabase` — [docs.rs fjall](https://docs.rs/fjall/latest/fjall/)

**redb (copy-on-write B+tree)**
- ACID, "crash-safe by default", copy-on-write B+trees, MVCC with concurrent readers and a writer — [redb README](https://github.com/cberner/redb)
- redb's own benchmark (Ryzen 9950X3D, Samsung 9100 PRO), in ms:

  | Benchmark | redb | lmdb | rocksdb | fjall | sqlite |
  |---|---|---|---|---|---|
  | Bulk load | 17063 | 9232 | 13969 | 18619 | 15341 |
  | Individual writes | 920 | 1598 | 2432 | 3488 | 7040 |
  | Batch writes | 1595 | 942 | 451 | 353 | 2625 |
  | Removals | 23297 | 10435 | 6900 | 6004 | 10323 |

  Uncompacted size is 4.00 GiB for redb vs 893 MiB for rocksdb (compacted: 1.69 GiB vs 455 MiB). The workload is small KV pairs, not 3-30 KB values — [redb README](https://github.com/cberner/redb)
- 4.5.0 (2026-11-05) added `ConcurrencyMode` (ExclusiveWriter default, SingleWriter, MultiWriter) — [redb CHANGELOG](https://raw.githubusercontent.com/cberner/redb/master/CHANGELOG.md)
- 2.3.0 (2024-12-10) added `set_two_phase_commit()` and `set_quick_repair()` (slower commits in exchange for faster repair). Durability::Paranoid was deprecated, then removed in 3.0.0 — [redb CHANGELOG](https://raw.githubusercontent.com/cberner/redb/master/CHANGELOG.md)

**surrealkv**
- LSM with a WAL and a lock-free ring-buffer commit pipeline. WiscKey-style vlog is opt-in (`with_enable_vlog(true)`, default threshold 1 KiB). Commits default to `Durability::Eventual`; `Durability::Immediate` waits for WAL sync. WAL recovery modes are `SalvageValidPrefix` (default) and `AbsoluteConsistency`. CI runs deterministic simulation (500 seeds × 5,000 steps). The README warns "on-disk formats are not frozen yet" — [surrealkv README](https://github.com/surrealdb/surrealkv)

**okaywal**
- WAL with preallocated segment files (`wal-{id}`). Concurrent commits share one `fsync`, and directory fsyncs are batched. Each chunk ends with a CRC-32. A background checkpointer calls a user `LogManager`, then recycles segments — [okaywal README](https://github.com/khonsulabs/okaywal)
- Its own benchmarks give average durable-write latency (1 → 16 threads): 4 KB 1.24 ms → 2.15 ms; 1 MB 7.02 ms → 19.01 ms — [okaywal README](https://github.com/khonsulabs/okaywal)

**sled**
- fsyncs every 500 ms by default (`flush_every_ms`), or explicitly via `flush()`/`flush_async()`. The README says sled "uses too much space sometimes" and recommends SQLite when reliability is the main concern — [sled README](https://github.com/spacejam/sled)

### Inferences
- For 3-30 KB immutable values that are never overwritten, BlobDB/fjall-blob/vlog GC would rarely relocate data unless retention deletes leave fragmented blob files. Write amplification would be close to 1x for values (WAL + blob file, so effectively 2x bytes unless the WAL is bypassed), plus LSM compaction of small pointers. A custom segment log or raft-engine writes each byte once to the log, with no separate WAL.
- At small value sizes, redb's copy-on-write B+tree writes about 2.4x more data on disk (uncompacted) than RocksDB. Each commit with fsync also rewrites pages along the path, so it is a poor fit as the bulk data store at hundreds of MB/s. It fits small, structured metadata/index better.
- precog's batch-per-group-commit model matches raft-engine's leader-writer queue almost exactly.

### Gaps
- No published benchmark was found for any of these engines at 3-30 KB values and hundreds of MB/s with per-group fsync. fjall 3's 4K-blob benchmark exists only as charts (no numbers in the text) — [Fjall 3.0 post](https://fjall-rs.github.io/post/fjall-3/)
- I did not verify RocksDB WAL group-commit internals or `manual_wal_flush` semantics from a primary source in this session.
- No primary source was fetched on fjall's internal group commit (whether concurrent `persist` calls coalesce into one fsync).

## Q2. Which provide cheap time/size retention (drop whole files) rather than tombstones + compaction?

### Takeaway
RocksDB FIFO compaction (size- and TTL-based whole-SST deletion) and raft-engine's `purge_expired_files()` drop whole files. okaywal recycles checkpointed segments. B-trees (redb, LMDB, SQLite) and fjall's current leveled LSM rely on per-key deletes (or an undocumented file-range drop in fjall), so retention costs I/O proportional to the data deleted.

### Cited Findings
- **RocksDB FIFO:** when total size exceeds `max_table_files_size`, the oldest SST file is deleted. The `ttl` option (since 5.7; in `ColumnFamilyOptions` since 6.0) deletes expired files, needs the block-based table format and `max_open_files=-1`, and falls back to size-based deletion. The wiki warns FIFO "can drop data without notifying users". The compaction filter is never applied — [FIFO wiki](https://github.com/facebook/rocksdb/wiki/FIFO-compaction-style)
- The BlobDB wiki does not mention FIFO compatibility and recommends leveled compaction — [BlobDB wiki](https://github.com/facebook/rocksdb/wiki/BlobDB)
- **raft-engine:** compaction is user-timed. Logically truncated entries leave holes, and `purge_expired_files()` physically removes fully-expired files, returning the Raft groups that block GC. TiKV calls it every 10 s by default — [raft-engine README](https://github.com/tikv/raft-engine)
- **fjall 3:** O(1) range deletion is listed as a missing feature. Snapshot reads are said to be unaffected by `drop_range` ("DeleteFilesInRange"), implying a file-level range drop exists but is undocumented. TTL-style cleanup is a planned compaction-filter use, and compaction filters shipped in 3.1 (2026-03-06) — [Fjall 3.0 post](https://fjall-rs.github.io/post/fjall-3/); [fjall blog index](https://fjall-rs.github.io/)
- **okaywal:** once a segment is checkpointed it is renamed with `-cp`, readers drain, and the file is recycled (whole-file reclaim) — [okaywal README](https://github.com/khonsulabs/okaywal)
- **redb:** the removals benchmark is the slowest of the five engines (23,297 ms vs RocksDB 6,900 ms) — [redb README](https://github.com/cberner/redb)
- New log crates with explicit retention: kafko ("topics, partitions, offset-based reads, replay, retention"), crabka/krabka-log (Kafka on-disk format, time- and size-based retention, truncation), wal-db (`truncate_before` on a segmented log) — [docs.rs kafko](https://docs.rs/kafko); [docs.rs crabka-log](https://docs.rs/crabka-log); [docs.rs wal-db](https://docs.rs/crate/wal-db/latest)

### Inferences
- Retention by time and size maps naturally onto immutable segment files. If the index lives in a separate KV store, retention means deleting a segment file plus a key-range delete in the index. In an LSM that range delete is cheap if it can use DeleteRange/DeleteFilesInRange; in redb it is O(keys deleted) of B-tree work. Sparse indexes (one entry per N KB) keep that small.

### Gaps
- Whether rust-rocksdb exposes FIFO TTL plus BlobDB together, and whether they interoperate, was not verified.
- fjall `drop_range` semantics and API stability were not verified from docs.

## Q3. Which are healthy projects to depend on as of Nov 2026?

### Takeaway
The active and released projects are rust-rocksdb (0.25.0, Aug 2026), fjall (3.1.13, Oct 2026), redb (4.6.0, Nov 13 2026), heed (0.23.1, Oct 2026), rusqlite (0.43.0, Nov 15 2026), surrealkv (1.4.0, Nov 2026, formats not frozen), foyer (0.22.6, Sep 2026) and persy (1.8.1, Jun 2026). raft-engine is maintained on git master but last released to crates.io in April 2024. sled, okaywal, nebari, marble, agatedb, commitlog and simple_wal are stale or dead.

### Cited Findings
Release data from the crates.io API, queried 2026-11-17:

| Crate | Latest version | Release date | License | Notes | Source |
|---|---|---|---|---|---|
| raft-engine | 0.4.2 | 2024-04-26 | Apache-2.0 | | [crates.io](https://crates.io/crates/raft-engine) |
| rocksdb (rust-rocksdb) | 0.25.0 | 2026-08-16 | Apache-2.0 | ~8.1M recent downloads | [crates.io](https://crates.io/crates/rocksdb) |
| fjall | 3.1.13 | 2026-10-25 | MIT OR Apache-2.0 | lsm-tree 3.1.10 released 2026-08-30 | [crates.io](https://crates.io/crates/fjall); [lsm-tree](https://crates.io/crates/lsm-tree) |
| redb | 4.6.0 | 2026-11-13 | MIT OR Apache-2.0 | ~7.0M recent downloads | [crates.io](https://crates.io/crates/redb) |
| sled | stable 0.34.7; newest 1.0.0-alpha.124 | alpha released 2024-10-11 | MIT OR Apache-2.0 | | [crates.io](https://crates.io/crates/sled) |
| okaywal | 0.3.1 | 2023-11-26 | | | [crates.io](https://crates.io/crates/okaywal) |
| nebari | 0.5.5 | 2023-02-27 | | | [crates.io](https://crates.io/crates/nebari) |
| heed | 0.23.1 | 2026-10-26 | MIT | | [crates.io](https://crates.io/crates/heed) |
| rusqlite | 0.43.0 | 2026-11-15 | MIT | | [crates.io](https://crates.io/crates/rusqlite) |
| surrealkv | 1.4.0 | 2026-11-11 | Apache-2.0 | | [crates.io](https://crates.io/crates/surrealkv) |
| canopydb | 0.2.5 | 2025-11-22 | MIT | low adoption (~7k downloads) | [crates.io](https://crates.io/crates/canopydb) |
| persy | 1.8.1 | 2026-06-30 | MPL-2.0 | hosted on GitLab | [crates.io](https://crates.io/crates/persy) |
| marble | 16.0.2 | 2025-02-28 | | | [crates.io](https://crates.io/crates/marble) |
| agatedb | 0.1.0 | 2020-10-09 | | effectively dead | [crates.io](https://crates.io/crates/agatedb) |
| foyer | 0.22.6 | 2026-09-09 | Apache-2.0 | a hybrid cache, not a durable store | [crates.io](https://crates.io/crates/foyer) |
| commitlog | 0.2.0 | 2021-11-06 | | | [crates.io](https://crates.io/crates/commitlog) |
| simple_wal | 0.3.0 | 2020-10-25 | | | [crates.io](https://crates.io/crates/simple_wal) |
| `wal` | 0.1.0 | 2026-03-09 | | 118 downloads, no repository | [crates.io](https://crates.io/crates/wal) |

Project activity and stability notes:
- **raft-engine git master:** commits on Feb 12, Mar 10, Apr 29, May 7 (fsync fix), Aug 24, Sep 10 2026, and Nov 17 2026 (an OWNERS auto-sync). Activity is low-volume maintenance, mostly fixes and OWNERS syncs — [raft-engine commits](https://github.com/tikv/raft-engine/commits/master)
- **raft-engine usage:** built for TiKV and the default TiKV log engine since v6.1 — [raft-engine README](https://github.com/tikv/raft-engine); [TiKV 6.1 notes](https://tikv.org/docs/6.1/new-features/overview/)
- **fjall release cadence:** 2.0 (2024-09-20), 2.2 concurrent write transactions (2024-10-20), 2.8 (2025-03-30), 3.0 "a year in the making" (2026-01-02), 3.1 compaction filters (2026-03-06). Terminology changed in v3 (Keyspace→Database, Partition→Keyspace) — [fjall blog](https://fjall-rs.github.io/); [daily.dev summary](https://app.daily.dev/posts/releasing-fjall-3-0-xler9vl7v). Sponsors are Orbitinghail and Svix; no named production users in the README — [fjall README](https://github.com/fjall-rs/fjall)
- **redb status:** README says "Stable and maintained", file format stable. The CHANGELOG lists an unreleased 5.0.0 with breaking changes — [redb README](https://github.com/cberner/redb); [CHANGELOG](https://raw.githubusercontent.com/cberner/redb/master/CHANGELOG.md)
- **redb has had a steady stream of serious durability bugs fixed through 2026:**
  - 2.1.2: transient I/O errors during commit could corrupt the database
  - 3.1.3 and 4.0.0 (2026-04-02): data loss when `AccessGuardMut` was dropped after commit
  - 4.2.0 (2026-08-17): two crashes could silently roll back durable commits, except with two-phase commit; database-growth crashes could make files unopenable
  - 4.3.0: crash-recovery rollback after an earlier repair
  - 4.6.0 (2026-11-13): repair could fall back to a commit slot that failed its checksum, opening a damaged file as empty

  Sources: [redb CHANGELOG](https://raw.githubusercontent.com/cberner/redb/master/CHANGELOG.md)
- **sled:** the README says "beta", "should be considered unstable", and that the on-disk format will change before 1.0 with manual migration. It also warns the README is out of sync with main, which holds a large rewrite (marble/komora) — [sled README](https://github.com/spacejam/sled)
- **okaywal:** the README says "early in development", do not use in production, file format unstable — [okaywal README](https://github.com/khonsulabs/okaywal)
- **surrealkv:** maintained by SurrealDB; "on-disk formats are not frozen yet" — [surrealkv README](https://github.com/surrealdb/surrealkv)
- **New 2025-2026 streaming/log crates (all tiny adoption):**
  - kafko 0.3.0, created 2026-05-24, 87 downloads — [crates.io](https://crates.io/crates/kafko)
  - crabka-log 0.4.1, renamed to krabka-log, Kafka-compatible on-disk format, about 1.6k downloads — [crates.io](https://crates.io/crates/crabka-log); [docs.rs](https://docs.rs/crabka-log)
  - seglog 0.3.6, created 2025-11, fixed-size segments with CRC-32, for event sourcing — [crates.io](https://crates.io/crates/seglog); [docs.rs README](https://docs.rs/crate/seglog/0.2.2/source/README.md)
  - wal-db 1.4.0, released 2026-11-12, "lock-free append path", about 2.8k downloads — [crates.io](https://crates.io/crates/wal-db)
  - walrus-rust 0.2.0, released 2025-10 — [crates.io](https://crates.io/crates/walrus-rust)
  - selium-log 0.0.0, 2024 — [crates.io](https://crates.io/crates/selium-log)
  - rumqtt's `segments` 0.3.0, 2021 — [crates.io](https://crates.io/crates/segments)
  - Apache Iggy, a full streaming server with an append-only log, very active (iggy_common release 2026-11-16) but not packaged as an embeddable storage crate — [crates.io iggy_common](https://crates.io/crates/iggy_common)

### Inferences
- **Bus factor:** fjall and redb appear to be essentially single-maintainer projects. The fjall README names no maintainers, and redb is under a personal account (cberner). This was not verified with contributor stats. RocksDB has Meta behind it, and the rust-rocksdb binding is community-maintained. raft-engine has PingCAP/TiKV behind it but receives little feature development.
- Depending on raft-engine means pinning a git revision, not a crates.io release, to get the 2026 fsync fix.
- None of the new 2025-2026 log crates is mature enough to depend on. They are useful only as design references.

### Gaps
- Last-commit dates, contributor counts, stars and open-issue counts for most repos were not collected because GitHub API access was blocked. The crates.io release dates above are the substitute signal.
- I found no verified production users of fjall, redb or surrealkv beyond what the READMEs say, which is none.
- sled issue-tracker specifics (data-loss reports, space blowup) were not collected beyond the README.
- I did not fetch primary docs for LMDB/heed durability, SQLite WAL `synchronous=FULL/NORMAL`, persy, canopydb, marble or foyer internals.

## Q4. Is a hybrid sensible (custom append-only segments for data + embedded KV for the index and metadata)?

### Takeaway
Yes. The evidence supports keeping the 3-30 KB batches in a purpose-built segment log, in the style of raft-engine or Kafka, with leader-based group commit and `fdatasync`. Retention then means unlinking whole files. A small embedded KV holds the sparse index and metadata, where write volume is small enough that the engine's write amplification does not matter. raft-engine itself is the strongest off-the-shelf alternative for the data path.

### Cited Findings
- PingCAP moved TiKV's Raft logs out of RocksDB because RocksDB's write amplification on log data consumed bandwidth and competed with flushes, a 25-40% I/O saving. User data stayed in RocksDB, which is a production precedent for a split data-log/KV design — [PingCAP write-up (search snippet)](https://pingcap.com/?p=9415); [AskTUG](https://ask.pingcap.com/t/what-is-the-underlying-storage-engine-in-tikv/1264)
- raft-engine already combines append-only files, an in-memory index of file locations, leader group commit with `fdatasync`, and file purge — [raft-engine README](https://github.com/tikv/raft-engine)
- **Index-store options:**
  - redb: fastest single-durable-commit latency in its own benchmark (individual writes 920 ms vs RocksDB 2,432 ms), stable file format, but a recent history of crash-recovery bugs — [redb README](https://github.com/cberner/redb); [CHANGELOG](https://raw.githubusercontent.com/cberner/redb/master/CHANGELOG.md)
  - fjall and RocksDB: best batch writes in that same table (353 / 451 ms) — [redb README](https://github.com/cberner/redb)

### Inferences
- A hybrid creates two durability domains. The index KV must either be committed in the same fsync group, which costs a second fsync per group, or be rebuildable from segment tails on recovery. The latter keeps the hot path at one fsync and bounds recovery to scanning only the unsealed segment(s). It also lowers the bar on index-engine durability, which mitigates the redb bug history.
- With sealed segments carrying their own sparse index footer files (Kafka-style), the embedded KV may only be needed for small metadata (segment catalog, writer epochs, high-water marks). That workload is small enough that redb, fjall or even SQLite would be adequate.
- Using RocksDB with FIFO + TTL for everything is plausible but adds a C++ dependency, a WAL that double-writes values, and FIFO's silent-drop semantics. Random reads by (log, seq) across many L0 files would also degrade without compaction, per the FIFO wiki's note about many L0 files slowing queries — [FIFO wiki](https://github.com/facebook/rocksdb/wiki/FIFO-compaction-style)

### Gaps
- I found no published head-to-head benchmark of "custom segment log + KV index" vs "LSM with blob separation" for broker-like workloads.
- raft-engine's recovery-time behaviour (it rebuilds memtables by scanning logs; no hint file) was not quantified by any source found.

# Production log/streaming storage engine designs (for precog)

Scope note: Primary sources only (source code, in-repo docs, official config references, vendor docs/blogs). The Apache Iggy findings come from the source tree at commit `793f0afb83944d7e2b754dd9b7d3c742b685dde9` (master, 2026-11-17). Where a GitHub link is given for Iggy, the file was read from that commit. Several systems in the brief could not be covered with primary sources within the research budget. They are listed under Gaps, not filled from memory.

Source shorthand:
- Iggy config: https://github.com/apache/iggy/blob/master/core/server/config.toml (the `[partition]` section, lines ~987-1210)
- Iggy index: https://github.com/apache/iggy/blob/master/core/partitions/src/iggy_index.rs
- Iggy writer: https://github.com/apache/iggy/blob/master/core/partitions/src/messages_writer.rs
- Iggy recovery: https://github.com/apache/iggy/blob/master/core/partitions/src/segment_recovery.rs
- Iggy WAL journal: https://github.com/apache/iggy/blob/master/core/journal/src/partition_journal.rs
- Kafka design doc: https://github.com/apache/kafka/blob/trunk/docs/design/design.md (rendered at https://kafka.apache.org/documentation/#design)
- Kafka log implementation doc: https://github.com/apache/kafka/blob/trunk/docs/implementation/log.md
- Kafka defaults: https://github.com/apache/kafka/blob/trunk/server-common/src/main/java/org/apache/kafka/server/config/ServerLogConfigs.java
- BookKeeper config reference: https://github.com/apache/bookkeeper/blob/master/site3/website/docs/reference/config.md (rendered at https://bookkeeper.apache.org/docs/reference/config)
- raft-engine README: https://github.com/tikv/raft-engine/blob/master/README.md
- TiKV config: https://docs.pingcap.com/tidb/stable/tikv-configuration-file/
- TigerBeetle safety: https://docs.tigerbeetle.com/concepts/safety/
- TigerBeetle VSR internals: https://github.com/tigerbeetle/tigerbeetle/blob/main/docs/internals/vsr.md
- TigerBeetle data file: https://github.com/tigerbeetle/tigerbeetle/blob/main/docs/internals/data_file.md
- Redpanda write caching: https://www.redpanda.com/blog/write-caching-performance-benchmark
- Redpanda fsync post: https://www.redpanda.com/blog/why-fsync-is-needed-for-data-safety-in-kafka-or-non-byzantine-protocols

---

## Q1. What is the common proven layout (one journal vs per-partition files; segment size; index granularity)?

### Takeaway
There are two proven families:
- **Log == data:** per-partition segment files plus a sparse index, as in Kafka and Iggy segments. Typical segments are about 1 GiB with roughly one index entry per 4 KiB (Kafka) or per flush chunk (Iggy).
- **Shared journal + separate data:** one sequential journal that all writers share (group commit), with data later written to a separate store (BookKeeper journal + entry log + RocksDB index; raft-engine one shared log stream + in-memory index; TigerBeetle WAL ring + grid).

Current Iggy is a hybrid. It has a per-partition WAL of "prepares" whose bodies are retained by hard links into the per-partition segment files, so payloads are not written twice. precog's single node-wide WAL with cross-writer group commit belongs to the BookKeeper/raft-engine family. Kafka and Iggy show how to lay out sealed segments and sparse indexes behind it.

### Cited Findings

**Apache Iggy (master, Nov 2026)**
- Every disk-backed partition uses a WAL for recovery and retries. "WAL references retain segment bodies by hard link until reclamation." — [Iggy config](https://github.com/apache/iggy/blob/master/core/server/config.toml)
- A WAL prepare body lives in a segment inode that the WAL retains. "Referenced bytes must remain unchanged until the WAL durably removes their operations." The caller must sync the body through its writing handle before it submits the reference. — [partition_journal.rs](https://github.com/apache/iggy/blob/master/core/journal/src/partition_journal.rs)
- The WAL metric `partition_wal_disk_bytes` "measures WAL file length, excluding referenced segment bodies". So the WAL file holds metadata and frames, and the bodies stay in the segments. — [Iggy config](https://github.com/apache/iggy/blob/master/core/server/config.toml)
- The WAL "frontier" is a two-slot checksummed block that is rewritten in place. Publication alternates slots, with a sequence counter inside the checksummed range. Older single-block magics (`IGGYWAL1`/`IGGYWAL2`/`IGGYWAL3`) are refused, so an old build cannot truncate the data file to a stale length and drop acknowledged records. — [partition_journal.rs](https://github.com/apache/iggy/blob/master/core/journal/src/partition_journal.rs)
- Segment size is set per topic (`segment_size`, default 1 GiB). It is a soft limit: a segment may close one whole batch past it. It must be a multiple of 512 B, at least 1 MiB, and at most 1 GiB. A layout that rolls by age also exists: a segment rolls when a batch's base timestamp runs past the active segment's first timestamp by the configured bound. — [Iggy config](https://github.com/apache/iggy/blob/master/core/server/config.toml); [segments.rs](https://github.com/apache/iggy/blob/master/core/journal/src/partition_journal/segments.rs)
- Sparse index entry: `{offset: u64, timestamp: u64, position: u64}`, little-endian, 24 bytes each (`IGGY_INDEX_SIZE = 3 * size_of::<u64>()`). This one index is both the offset index and the time index. — [iggy_index.rs](https://github.com/apache/iggy/blob/master/core/partitions/src/iggy_index.rs)
- Index granularity: "Persistence writes exactly one entry per flush chunk and chunks never overlap." — [segment_recovery.rs](https://github.com/apache/iggy/blob/master/core/partitions/src/segment_recovery.rs)
- Segment flush thresholds: `messages_required_to_save` defaults to 1024 and `size_of_messages_required_to_save` to 1 MiB. "Required persistence, capacity pressure and lifecycle operations can flush sooner." — [Iggy config](https://github.com/apache/iggy/blob/master/core/server/config.toml)
- Per-partition WAL budget `wal_bytes_max` defaults to 256 MiB. It must be a 4 KiB multiple between 128 MiB + 8 KiB and 4 GiB. Checkpointing starts at half the budget, or sooner on an explicit request or once 1024 files are dirty. A single persisted prepare is limited to 64 MiB. — [Iggy config](https://github.com/apache/iggy/blob/master/core/server/config.toml)
- The I/O runtime is compio (`compio = "0.19.2"` in the workspace Cargo.toml), a completion-based (io_uring on Linux) runtime. — [Iggy Cargo.toml](https://github.com/apache/iggy/blob/master/Cargo.toml)

**Apache Kafka**
- "The message log maintained by the broker is itself just a directory of files, each populated by a sequence of message sets that have been written to disk in the same format used by the producer and consumer." — [Kafka design.md](https://github.com/apache/kafka/blob/trunk/docs/design/design.md)
- A read locates the segment file, computes the file-specific offset, and reads from it. "The search is done as a simple binary search variation against an in-memory range maintained for each file." — [Kafka log.md](https://github.com/apache/kafka/blob/trunk/docs/implementation/log.md)
- Defaults: `log.index.interval.bytes` = 4096 (one sparse index entry per ~4 KiB of log), `log.index.size.max.bytes` = 10 MiB, `log.retention.bytes` = -1 (unlimited), cleanup interval 5 min. — [ServerLogConfigs.java](https://github.com/apache/kafka/blob/trunk/server-common/src/main/java/org/apache/kafka/server/config/ServerLogConfigs.java)
- Batches are protected by a CRC-32C covering attributes to end of batch. The partition leader epoch is excluded from the CRC so it can be assigned without recomputing. — [Kafka message-format doc](https://github.com/apache/kafka/blob/trunk/docs/implementation/message-format.md)

**Apache BookKeeper (shared journal + entry log + index)**
- The journal is the write-ahead log (`journalDirectories`, multiple allowed). Journal files roll at `journalMaxSizeMB` = 2048. — [BookKeeper config](https://bookkeeper.apache.org/docs/reference/config)
- The entry logger interleaves entries from many ledgers into entry log files of `logSizeLimit` = 2147483648 (2 GiB). `entryLogFilePreallocationEnabled` = true. — [BookKeeper config](https://bookkeeper.apache.org/docs/reference/config)
- DbLedgerStorage keeps ledger indexes in RocksDB. The block cache default is 256 MiB, "big enough to hold a significant portion of the index database which can reach ~2GB in some cases". Write buffer 64 MB, SST 64 MB, block size 64 KiB, bloom filter at 10 bits per key. — [BookKeeper config](https://bookkeeper.apache.org/docs/reference/config)
- The entry log is garbage-collected by compaction. Minor compaction runs at < 20% live data (`minorCompactionThreshold` 0.2) and major at a higher threshold. — [BookKeeper config](https://bookkeeper.apache.org/docs/reference/config)

**TiKV raft-engine (shared log stream, in-memory index)**
- "Raft Engine is a persistent embedded storage engine with a log-structured design similar to bitcask." "Each Raft Group holds its own memtable, containing all the key value pairs and the file locations of all log entries." "Different Raft Groups share the same log stream." — [raft-engine README](https://github.com/tikv/raft-engine/blob/master/README.md)
- Because the stream is shared, truncated logs "punch holes in the log files". GC compacts files when the user calls `purge_expired_files()`, which TiKV does every 10 s by default. GC returns the list of Raft groups whose old entries block reclamation, so the caller can force compaction of those groups. — [raft-engine README](https://github.com/tikv/raft-engine/blob/master/README.md)
- TiKV defaults:
  - `raft-engine.target-file-size` = 128 MiB (rotation size).
  - `purge-threshold` = 10 GiB (main log queue size that triggers purge).
  - `batch-compression-threshold` = 4 KiB (8 KiB before v8.1).
  - `enable-log-recycle` (since v6.3) reserves purged files for reuse "to reduce the long tail latency on write workloads".
  - `prefill-for-recycle` arrived in v7.0.

  — [TiKV config](https://docs.pingcap.com/tidb/stable/tikv-configuration-file/)

**TigerBeetle (fixed-size WAL ring + grid in one data file)**
- "WAL: Write-ahead log. It is implemented as two on-disk ring buffers. Entries are only overwritten after they have been checkpointed." The data file is made of zones, and the superblock is one zone at a fixed position. — [TigerBeetle vsr.md](https://github.com/tigerbeetle/tigerbeetle/blob/main/docs/internals/vsr.md); [data_file.md](https://github.com/tigerbeetle/tigerbeetle/blob/main/docs/internals/data_file.md)
- Grid blocks are addressed by `(u64 index, u128 checksum)`. "The block checksum is stored outside of the block itself, to protect from misdirected writes." — [data_file.md](https://github.com/tigerbeetle/tigerbeetle/blob/main/docs/internals/data_file.md)

### Inferences
- For precog's single node-wide WAL with ~3-30 KB batches, BookKeeper and raft-engine are the closest analogues. Both had to solve GC of interleaved data: BookKeeper by entry-log compaction, raft-engine by hole-punching compaction plus feedback naming the laggard groups. precog will hit the same problem if retention differs per stream inside one shared file. Options: (a) like Iggy, use the WAL only as a short-lived journal and materialize per-log segments, or (b) accept compaction.
- Iggy's hard-link trick lets the WAL reference segment bodies without a second copy. It could port to precog if WAL payloads are written once into a per-log segment and the WAL records only references. The cost is many fsync targets per group commit, one per touched segment file. That works against the single-journal group commit precog wants.
- Index granularity in practice is "one entry per ~4 KiB" (Kafka) or "one entry per flush chunk" (Iggy). Both are sparse enough to mmap or keep in RAM for a 1 GiB segment. Kafka's 10 MiB index cap / 8-byte entries ≈ 1.3M entries per segment.

### Gaps
- Kafka's `log.segment.bytes` default (widely documented as 1 GiB) and the `.timeindex` entry format were not extracted from source in this session. The defaults file read covers only index and flush settings.
- I did not obtain primary sources for Redpanda's segment layout, FoundationDB TLog/DiskQueue, LogDevice LogsDB, S2, AutoMQ WAL, or Pulsar specifics.

---

## Q2. Who fsyncs per write vs relies on replication, and what performance do they report?

### Takeaway
- **Kafka** deliberately disables application fsync by default and relies on replication plus the page cache. Its docs claim per-write fsync "can reduce performance by two to three orders of magnitude".
- **BookKeeper, raft-engine, TigerBeetle and Iggy's "persisted" mode** fsync (fdatasync) before ack, amortized by group commit. BookKeeper waits up to 2 ms to group journal force-writes.
- **Redpanda** fsyncs before ack with acks=all by default. Its opt-in "write caching" mode drops that and reports a >90% p99 latency reduction on EBS (113 ms to 8.5 ms).
- **Iggy** makes the choice per topic: "replicated" (default, no stable-storage barrier before ack) vs "persisted". It adds an optional bounded group-commit delay, `wal_group_commit_delay_micros`.

### Cited Findings

**Kafka**
- "we do not want to require the use of fsync on every write for our consistency guarantees as this can reduce performance by two to three orders of magnitude. Our protocol for allowing a replica to rejoin the ISR ensures that before rejoining, it must fully re-sync again even if it lost unflushed data in its crash." — [Kafka design.md](https://github.com/apache/kafka/blob/trunk/docs/design/design.md)
- "We recommend using the default flush settings which disable application fsync entirely ... We generally feel that the guarantees provided by replication are stronger than sync to local disk." Application-level flush "can introduce latency as fsync in most Linux filesystems blocks writes to the file whereas the background flushing does much more granular page-level locking." — [Kafka docs (operations)](https://github.com/apache/kafka/tree/trunk/docs) (quote found in the in-repo docs; rendered at https://kafka.apache.org/documentation/#appvsosflush)
- Defaults: `log.flush.interval.messages` = Long.MAX_VALUE and the flush scheduler interval = Long.MAX_VALUE, so Kafka never fsyncs by count or time. `log.flush.offset.checkpoint.interval.ms` = 60000. — [ServerLogConfigs.java](https://github.com/apache/kafka/blob/trunk/server-common/src/main/java/org/apache/kafka/server/config/ServerLogConfigs.java)

**Redpanda**
- With acks=all and write caching off, data is flushed to disk on the replicas before the client is acknowledged. Write caching (`write.caching` per topic, `write_caching_default` cluster-wide) acks after replication but before fsync, deferring writes until a buffer size or flush interval is reached. `flush.ms` / `flush.bytes` control the `fdatasync()` cadence. The stated risk is losing a small amount of data on a correlated failure of a majority of a partition's replicas. — [Redpanda write-caching blog](https://www.redpanda.com/blog/write-caching-performance-benchmark)
- Reported results (9 × im4gen.8xlarge, OMB, idempotence on, acks=all):
  - EBS p99 fell from ~113 ms to ~8.5 ms, a reduction of more than 90%.
  - In a saturation test, NVMe and EBS both reached 33 ms p99 with write caching.
  - CPU use dropped about 10%.

  — [Redpanda write-caching blog](https://www.redpanda.com/blog/write-caching-performance-benchmark)
- Redpanda's argument against Kafka's approach: without fsync, a machine or OS crash or power loss "can still" lose recently written data, and replication alone does not cover correlated failures. — [Redpanda fsync blog](https://www.redpanda.com/blog/why-fsync-is-needed-for-data-safety-in-kafka-or-non-byzantine-protocols)

**BookKeeper**
- `journalSyncData`: "Should the data be fsynced on journal before acknowledgment. By default, data sync is enabled to guarantee durability of writes." Group-commit knobs:
  - `journalAdaptiveGroupWrites` = true ("group journal force writes, which optimize group commit for higher throughput").
  - `journalMaxGroupWaitMSec` = 2 ms.
  - `journalBufferedWritesThreshold` = 524288 bytes.
  - `journalFlushWhenQueueEmpty` = false.

  — [BookKeeper config](https://bookkeeper.apache.org/docs/reference/config)
- The entry log is flushed separately: `flushEntrylogBytes` triggers incremental flushes, and when it is ≤0 the flush happens on rotation. A write cache (`dbStorage_writeCacheMaxSizeMb`, default 25% of direct memory) buffers entries before they reach the entry log. — [BookKeeper config](https://bookkeeper.apache.org/docs/reference/config)

**raft-engine**
- Writers queue, and "the first in line automatically becomes the queue leader, responsible for writing the entire group to the log file. ... When one write in a batch is marked synchronous, the batch leader will call `fdatasync()` after writing." — [raft-engine README](https://github.com/tikv/raft-engine/blob/master/README.md)

**TigerBeetle**
- "Each operation is stored in a quorum of replicas' WALs before commit". A replica sends `prepare_ok` "once it has written the prepare to the WAL". — [TigerBeetle safety](https://docs.tigerbeetle.com/concepts/safety/); [vsr.md](https://github.com/tigerbeetle/tigerbeetle/blob/main/docs/internals/vsr.md)

**Iggy**
- Both durability policies write data to storage. "'replicated' waits for VSR commit and application without an additional stable-storage barrier." "'persisted' also requires recoverable stable-storage copies at the replication quorum before explicit success." Both `durability` and `consumer_offset_durability` default to "replicated". — [Iggy config](https://github.com/apache/iggy/blob/master/core/server/config.toml)
- After a crash, a "replicated" partition recovers up to the last WAL frontier it published. That happens at checkpoints, durable operations and graceful shutdown. — [Iggy config](https://github.com/apache/iggy/blob/master/core/server/config.toml)
- `wal_group_commit_delay_micros` (default 0, max 10000) is a bounded wait for more prepares before the WAL writer starts its durability barrier. With it, "one barrier and one frontier write then cover a whole group of prepares". The wait is skipped while prepares arrive further apart than the delay, so an idle partition never waits. The docs recommend starting near the measured barrier duration. Metrics `partition_wal_prepares_total / partition_wal_batches_total` report prepares per durable group. — [Iggy config](https://github.com/apache/iggy/blob/master/core/server/config.toml)
- Segment flush uses `fdatasync` rather than `fsync` because "segment files are append-only and the size change is tracked in datasync metadata on Linux". The inode metadata sync of `sync_all` "adds latency without correctness gain". — [messages_writer.rs](https://github.com/apache/iggy/blob/master/core/partitions/src/messages_writer.rs)
- Single-replica partitions reserve offsets in a durable superblock ahead of use (`offset_reservation_lease` = 65536), so a restarted replica never re-mints an offset it already confirmed. Replicated groups skip this, because re-minting there would need a full-cluster crash. — [Iggy config](https://github.com/apache/iggy/blob/master/core/server/config.toml)

### Inferences
- The proven pattern for "fdatasync before ack" at good throughput is leader-based group commit: one writer drains a queue, does one write, does one fdatasync, then wakes all waiters. BookKeeper and raft-engine do exactly this. Iggy's adaptive delay adds a bounded wait that is used only when arrivals are dense, as does BookKeeper's 2 ms max group wait. precog's design matches this family.
- Use `fdatasync` and preallocate (or extend file size rarely) so that each sync does not also commit a size-metadata change.

### Gaps
- No primary-source per-fsync latency or throughput numbers were gathered for BookKeeper, raft-engine or TigerBeetle.
- Redpanda's default `flush.ms` / `flush.bytes` values were not given on the page fetched.

---

## Q3. Who uses page cache vs O_DIRECT and why?

### Takeaway
- **Kafka** relies on the page cache and sendfile: a doubled effective cache, a cache that survives restarts, and zero-copy reads for caught-up consumers.
- **TigerBeetle** uses O_DIRECT with its own cache, since it treats the disk as faulty and must control durability exactly.
- **BookKeeper** offers an optional direct-I/O entry logger, off by default. **Iggy** uses buffered I/O with fdatasync. No O_DIRECT use was found in the source.

### Cited Findings
- Kafka: the page cache "cannot easily be turned off without using direct I/O, so even if a process maintains an in-process cache ... this data will likely be duplicated in OS pagecache." Relying on it yields "a cache of up to 28-30GB on a 32GB machine without GC penalties", and "this cache will stay warm even if the service is restarted". — [Kafka design.md](https://github.com/apache/kafka/blob/trunk/docs/design/design.md)
- Kafka: "on a Kafka cluster where the consumers are mostly caught up you will see no read activity on the disks whatsoever as they will be serving data entirely from cache". sendfile is not used when TLS is enabled. — [Kafka design.md](https://github.com/apache/kafka/blob/trunk/docs/design/design.md)
- TigerBeetle "writes data to disk with O_DIRECT", manages its own page cache, can run on a raw block device, and uses io_uring. — [TigerBeetle safety](https://docs.tigerbeetle.com/concepts/safety/)
- BookKeeper: `dbStorage_directIOEntryLogger` defaults to false, with an 8 MB read buffer per direct reader. It has its own read-ahead cache, `dbStorage_readAheadCacheMaxSizeMb` (25% of direct memory). — [BookKeeper config](https://bookkeeper.apache.org/docs/reference/config)
- TiKV/RocksDB: `use-direct-io-for-flush-and-compaction` defaults to false. O_DIRECT "bypasses and prevents contamination of the OS buffer cache, but the subsequent file reads require re-reading the contents". — [TiKV config](https://docs.pingcap.com/tidb/stable/tikv-configuration-file/)
- Iggy: no `O_DIRECT` or `custom_flags` found in `core/` (grep of the source tree at the commit above). Its sizing notes describe serving caches as sitting "on top of page cache". `transfer_served_cache_bytes_max` = 2176 MiB per shard. — [Iggy config](https://github.com/apache/iggy/blob/master/core/server/config.toml)

### Inferences
- For precog, the tail-read path (live subscribers) is best served from memory: an in-process ring of recent batches, or the page cache. Kafka's argument holds if consumers read recent data. O_DIRECT pays off mainly when you need deterministic control (TigerBeetle) or want to avoid page-cache pollution from catch-up reads of cold data. A common middle ground is buffered writes + fdatasync for the WAL plus `posix_fadvise(DONTNEED)` on sealed segments. This is an inference; that middle ground was not sourced.

### Gaps
- Redpanda's DMA/O_DIRECT, per-core batch cache and fallocate details: the page (https://www.redpanda.com/blog/tpc-buffers) rendered without body text, so these could not be verified.

---

## Q4. How do they get fast recovery (checkpoints, index rebuild bounded to last segment)?

### Takeaway
Every system bounds recovery with a durable "recovery point":
- **Kafka** checkpoints a per-partition recovery point every 60 s and validates the newest segment with a CRC scan, truncating at the first invalid batch.
- **TigerBeetle** replays the WAL from the last superblock checkpoint.
- **raft-engine** offers recovery modes for corrupt tails.
- **Iggy** anchors recovery on index entries whose existence proves the bytes were fsynced. It warns that with "replicated" durability (no barrier), writeback reordering forces a scan from byte zero.

### Cited Findings
- Kafka: "On startup a log recovery process is run that iterates over all messages in the newest log segment and verifies that each message entry is valid", meaning size+offset is within the file length and the CRC32 matches. "In the event corruption is detected the log is truncated to the last valid offset." — [Kafka log.md](https://github.com/apache/kafka/blob/trunk/docs/implementation/log.md)
- Kafka on why CRC matters: "the OS makes no guarantee of the write order between the file inode and the actual block data so in addition to losing written data the file can gain nonsense data if the inode is updated with a new size but a crash occurs before the block containing that data is written." — [Kafka log.md](https://github.com/apache/kafka/blob/trunk/docs/implementation/log.md)
- Kafka: `log.flush.offset.checkpoint.interval.ms` = 60000 and `num.recovery.threads.per.data.dir` = 2. — [ServerLogConfigs.java](https://github.com/apache/kafka/blob/trunk/server-common/src/main/java/org/apache/kafka/server/config/ServerLogConfigs.java)
- TigerBeetle: "The WAL is the source of truth and is replayed from the last on-disk checkpoint at startup." A checkpoint ensures all updates from the past WAL wrap are durable in the grid, then advances the recovery point in the superblock. The superblock is stored as `config.superblock_copies` copies. — [TigerBeetle safety](https://docs.tigerbeetle.com/concepts/safety/); [vsr.md](https://github.com/tigerbeetle/tigerbeetle/blob/main/docs/internals/vsr.md)
- Iggy recovery rules:
  - It "validates message batches against their checksums and partition identity before serving data".
  - "A genuinely incomplete tail can be truncated. Interior damage or valid records after a damaged range require refusal or repair, never blind truncation."
  - "With message durability = 'replicated', background writeback can preserve later pages before earlier ones, so recovery checks the log from byte zero." With "persisted", "completed durable segment flushes constrain the incomplete tail and permit the established index-anchored recovery path".
  - "Unrecoverable local segment data is fenced and repaired from peers when available."

  — [Iggy config](https://github.com/apache/iggy/blob/master/core/server/config.toml)
- Iggy's index-anchored proof: the WAL makes a body durable before acknowledging it, and the flush that indexes it runs later. So "an entry existing on disk proves the log bytes it names were already fdatasynced". At most one index entry (`MAX_FSYNCED_INDEX_STEP_BACK_ENTRIES = 1`) may legitimately be unbacked. A deeper step-back is treated as evidence of fsynced-log loss (`FsyncedLogLoss` refusal). — [segment_recovery.rs](https://github.com/apache/iggy/blob/master/core/partitions/src/segment_recovery.rs)
- Iggy: "Index reconstruction and validation can take time across many segments. A slow recovery is not by itself evidence of a stalled server." — [Iggy config](https://github.com/apache/iggy/blob/master/core/server/config.toml)
- TiKV raft-engine exposes `recovery-mode` (how to handle file corruption during recovery), `recovery-read-block-size` and `recovery-threads`. — [TiKV config](https://docs.pingcap.com/tidb/stable/tikv-configuration-file/)

### Inferences
- A pattern for precog:
  1. Persist a small, double-buffered, checksummed checkpoint (Iggy's two-slot frontier, TigerBeetle's superblock copies). It records the WAL position up to which all sealed-segment and index state is durable.
  2. On restart, replay only the WAL suffix after that point.
  3. Trust sealed segments plus their index files, verifying a footer or checksum, without rescanning.
  4. Rebuild the index only for the active segment.
- Distinguish a torn tail (truncate) from interior corruption (refuse or repair from peers). Iggy and Protocol-Aware Recovery both insist on this distinction.

### Gaps
- No measured recovery times (seconds per GB) were found for any system.

---

## Q5. What incidents/bugs informed their designs?

### Takeaway
The recurring lessons:
- **Fsync can fail or lie.** Fsyncgate and the TigerBeetle fault model are the references here.
- **File size/inode metadata can be persisted before data.** Kafka's CRC rationale and Iggy's "check from byte zero" rule both respond to this.
- **Disks misdirect writes and corrupt silently.** TigerBeetle stores checksums out-of-band, and Protocol-Aware Recovery uses replicas to repair rather than truncating acknowledged data.
- **Format evolution can make old binaries truncate acknowledged data.** Iggy changed its WAL magic for this reason.

### Cited Findings
- TigerBeetle assumes its disk will fail. Named threats are silent corruption, misdirected I/O, and gray failure (very slow disks without errors). Handling fsync failure correctly is "particularly hard", and the docs cite the Fsyncgate research and Protocol-Aware Recovery (FAST '18). — [TigerBeetle safety](https://docs.tigerbeetle.com/concepts/safety/); [PAR paper](https://www.usenix.org/system/files/conference/fast18/fast18-alagappan.pdf)
- TigerBeetle's VOPR runs a whole cluster of real code under network, storage and process faults at about 1000x speed, 24/7 on 1024 cores. — [TigerBeetle safety](https://docs.tigerbeetle.com/concepts/safety/)
- Kafka: "disk errors are the most common problem we observe in real operation of persistent data systems and they often do not leave data intact", hence a replica must fully re-sync before rejoining the ISR. — [Kafka design.md](https://github.com/apache/kafka/blob/trunk/docs/design/design.md)
- Iggy: a pre-two-slot build would read block 0 and "truncate the data file to the length it finds there". After an ack lands in slot 1, that block is one publication stale. The new magic makes old builds refuse to open rather than drop acknowledged records. — [partition_journal.rs](https://github.com/apache/iggy/blob/master/core/journal/src/partition_journal.rs)
- Iggy: "Recovery must not replace an acknowledged durable history with an empty healthy partition". Fenced files remain for diagnosis. — [Iggy config](https://github.com/apache/iggy/blob/master/core/server/config.toml)
- Iggy: the state-transfer artifact cap must cover the largest segment plus one max message. Otherwise "a legal segment is refused, the whole manifest with it, and the partition livelocks re-requesting it from every peer". This shows the cost of soft segment-size limits. — [Iggy config](https://github.com/apache/iggy/blob/master/core/server/config.toml)
- raft-engine's log recycling (TiKV v6.3+) exists to "reduce the long tail latency on write workloads" by reusing already-allocated files instead of creating new ones. — [TiKV config](https://docs.pingcap.com/tidb/stable/tikv-configuration-file/)

### Inferences
- For precog:
  - Use CRC-32C or a stronger checksum per batch, plus a hash-chain or prev-checksum in WAL frames, so you can tell torn tails from interior corruption.
  - Treat an fsync error as fatal: crash and recover rather than retry, per the Fsyncgate lesson.
  - Pre-allocate or recycle WAL files to avoid metadata-on-sync costs and tail latency.
  - Keep sealed-segment size hard-bounded so that all downstream buffers can be sized from it.

### Gaps
- No primary sources were gathered on why TiKV moved from RocksDB to raft-engine (write amplification and compaction stalls are the commonly cited reasons) or on any performance figures from that move. The PingCAP/TiKV blog URLs tried returned 404 or a different post.
- Not covered with primary sources: FoundationDB TLog/DiskQueue (SIGMOD 2021 paper), LogDevice/LogsDB, S2 (s2.dev), Pulsar/AutoMQ WAL, Kafka KIP-405 tiered storage, and academic work on SSD/ZNS logs. All of these need a follow-up pass.
- Redpanda Seastar/DMA/fallocate/batch-cache specifics are unverified (blog body not retrievable as text).

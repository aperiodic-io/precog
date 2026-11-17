# Raw file I/O approaches from Rust for a durable append-only WAL, and Linux durability gotchas

Scope: I/O primitives for precog's per-node group-commit WAL (one writer thread, immutable batches of about 3-30 KB, fdatasync before ack) and its sealed segments. Notes are current to 2026-11-17. Crate data comes from the crates.io API, queried 2026-11-17. The GitHub API was not reachable from this session, so last-commit dates for repos are missing unless noted.

Baseline from the precog test VM (ext4 on virtio): fdatasync of a 27 KB append to a growing file had p50 297 µs. To a preallocated, zeroed file it had p50 123 µs. The findings below explain this 2.4x gap: appending changes i_size, and writing into unwritten extents converts them. Both are metadata changes that force a journal commit inside fdatasync.

## 1. std::fs write + fdatasync; append vs. pwrite into a preallocated file; fallocate; file recycling

### Takeaway
For a group-commit WAL, plain buffered `pwrite` + `fdatasync` (`File::sync_data`) into a file that is preallocated **and pre-written with zeros** (or recycled) is the simplest durable option. It avoids the two metadata costs that turn fdatasync into a journal commit: i_size growth and unwritten-extent conversion. `posix_fallocate`/`FALLOC_FL_KEEP_SIZE` alone does not remove the conversion cost on ext4/XFS.

### Cited Findings
- fdatasync() is like fsync() but skips metadata "unless that metadata is needed in order to allow a subsequent data retrieval to be correctly handled." The man page's example: a file-size change requires a metadata flush, but an mtime change does not. — [fsync(2) man page](https://man7.org/linux/man-pages/man2/fsync.2.html)
- So an appending write (i_size grows) makes every fdatasync also commit the inode and journal. An overwrite inside an already-allocated, already-written region makes fdatasync a data write plus a cache flush. — inference from [fsync(2)](https://man7.org/linux/man-pages/man2/fsync.2.html); consistent with precog's 297 µs vs 123 µs measurement.
- On ext4 and XFS, fallocate creates blocks in the **unwritten** state. A benchmark posted to linux-ext4 found O_DSYNC overwrites of a preallocated unwritten file much slower than of a written file: about 20.1k IOPS unwritten, 42.9k with ext4 `fast_commit`, and 98.8k for an already-written file. — [linux-ext4 / LWN patch discussion (FALLOC_FL_WRITE_ZEROES RFC, Jan 2025)](https://lists.infradead.org/pipermail/linux-nvme/2025-January/053398.html); [LWN 1005186](https://lwn.net/Articles/1005186/)
- In a 2022 linux-ext4 thread, ftrace showed `ext4_convert_unwritten_extents()` taking a lot of time on first write to preallocated space. The call is absent once the file has been written. — [linux-ext4 2022-06-17](https://lists.openwall.net/linux-ext4/2022/06/17/13); [follow-up](https://lists.openwall.net/linux-ext4/2022/06/20/24)
- ext4 developer Andreas Dilger described the approach used by enterprise databases: fallocate a large chunk (for example 32 MB), then fdatasync once, so the extent tree is updated once rather than per write. ext4 also zero-fills up to `extent_max_zeroout_kb` (default 32 KB) when splitting extents, to avoid extent fragmentation. — [linux-ext4 2022 thread](https://lists.openwall.net/linux-ext4/2022/06/23/15)
- Writing real zeros (for example with dd) is the traditional way to get *written* extents, but it costs disk bandwidth. Kernel patches by Zhang Yi propose `FALLOC_FL_WRITE_ZEROES` so filesystems can allocate written extents (using device write-zeroes/unmap offload). — [RFC v2 0/8 FALLOC_FL_WRITE_ZEROES](https://lists.infradead.org/pipermail/linux-nvme/2025-January/053398.html); [LWN 1005186](https://lwn.net/Articles/1005186/); [LWN 1014553](https://lwn.net/Articles/1014553/). (Merge status not confirmed by my sources; see Gaps.)
- PostgreSQL does exactly this. `wal_init_zero` (default on, PG 12+) fills new WAL segment files with zeros so space is allocated before WAL records are written. `wal_recycle` (default on) renames old segments for reuse instead of creating new ones. The docs note that copy-on-write filesystems may be faster with both off. — [PostgreSQL docs: WAL configuration](https://www.postgresql.org/docs/current/runtime-config-wal.html)
- RocksDB `recycle_log_file_num` keeps old WAL files for reuse. Per the RocksDB WAL-Performance wiki (quoted on ceph-users), a reused file is written from offset 0, so the file size does not change until writes pass the old end. That avoids metadata I/O, depending on mount options. — [ceph-users thread quoting RocksDB wiki](https://lists.ceph.io/hyperkitty/list/ceph-users@ceph.io/thread/RD5COVDHLVEMIVTFOXHOO4DKTAFFGGQB/); [RocksDB WAL-Performance wiki](https://github.com/facebook/rocksdb/wiki/WAL-Performance)
- Recycling has a recovery hazard. Stale records from the file's previous life sit past the new tail. RocksDB 6.7 disabled recycling under `kPointInTimeRecovery`/`kAbsoluteConsistency`, and 6.13 sanitized it to 0 under `kTolerateCorruptedTailRecords`. RocksDB's recyclable record format embeds the log number in each record header so a reader can tell stale records from current ones. — [RocksDB commit #6351 mirror](https://git.nextgraph.org/NextGraph/rocksdb/commit/3316d29221f12596663b9ed28a153328674b3de9); [commit #7271 mirror](https://git.nextgraph.org/NextGraph/rocksdb/commit/5d5ff8240852c484c43580f017a831bbf306fb0d); [RocksDB HISTORY.md mirror](https://rust-digger.code-maven.com/src/rocks-sys-0.1.10/rocksdb/HISTORY.md)
- RocksDB PR #12591 fixed a bug where turning recycling from 0 to non-zero let old-format log files from a previous session be reused after reopen. — [db_write_test.cc (fork mirror)](https://forgejo.ellis.link/continuwuation/rocksdb/src/branch/v9.2.1/db/db_write_test.cc)
- Rust API surface. `std::fs::File::sync_data` maps to fdatasync on Linux and `sync_all` maps to fsync. `std::os::unix::fs::FileExt::write_at` is pwrite. For fallocate, pwritev2, sync_file_range, and O_DIRECT open flags you need `rustix` (latest 1.1.6, 2026-10-27), `nix` (latest 0.32.0, 2026-11-17), or `libc` (latest stable 0.2.x, with 1.0.0-alpha.5 on 2026-10-16). — [crates.io API: rustix](https://crates.io/crates/rustix), [nix](https://crates.io/crates/nix), [libc](https://crates.io/crates/libc); [std::fs::File docs](https://doc.rust-lang.org/std/fs/struct.File.html)

### Inferences
- Recommended baseline for precog: size each segment (for example 64-256 MB) and preallocate it with written zeros, either when the segment is created on a background thread or by recycling a sealed, no-longer-needed segment. Then pwrite at a tracked offset and fdatasync. This keeps i_size and extents stable, so fdatasync is "data + FLUSH" only. That matches the 123 µs case on the VM.
- With recycled or pre-zeroed files, the recovery scanner can no longer use EOF to find the tail. It must find the tail from record framing: length + CRC + monotonically increasing LSN/epoch (or segment generation number). A zero-filled region gives a clean "zero length = end" sentinel. A recycled region needs a generation or segment ID in each record header (RocksDB's approach) so stale records fail validation.
- Keep a spare pre-zeroed segment ready so segment rollover never sits on the commit path.

### Gaps
- Whether `FALLOC_FL_WRITE_ZEROES` merged into mainline, and in which version, is not confirmed by my sources. LWN coverage (1005186/1014553/1023811) suggests it was progressing in 2025. Verify before depending on it.
- I found no published benchmark that isolates RocksDB `recycle_log_file_num` gains in µs.

## 2. O_DIRECT, O_DSYNC / RWF_DSYNC (pwritev2), alignment, FUA; who uses it

### Takeaway
O_DIRECT bypasses the page cache. That is why TigerBeetle and Redpanda/Seastar use it, partly to avoid the page-cache-after-EIO problems of fsyncgate. But O_DIRECT is **not** durability by itself: you still need O_DSYNC/RWF_DSYNC (which the kernel may turn into a FUA write) or fdatasync. It also imposes alignment rules (logical block size, usually 512 B or 4 KiB) and needs your own buffer management.

### Cited Findings
- O_DIRECT tries to minimize cache effects, but writes are synchronous only with respect to the transfer, and it "does not give the guarantees of O_SYNC". Use O_SYNC/O_DSYNC or an fsync call to make data durable. Alignment of the buffer, offset, and length to the device's logical block size is required. Since Linux 6.1, `statx()` with `STATX_DIOALIGN` reports the required alignment per file. — [open(2) man page, O_DIRECT section](https://man7.org/linux/man-pages/man2/open.2.html); [statx(2)](https://man7.org/linux/man-pages/man2/statx.2.html)
- `pwritev2()` with `RWF_DSYNC` (Linux 4.7+) gives per-write O_DSYNC semantics without opening the file O_DSYNC. — [readv(2)/pwritev2 man page](https://man7.org/linux/man-pages/man2/pwritev2.2.html)
- Third-party caution (HN discussion): O_DIRECT skipping the page cache does not mean the drive's volatile cache was flushed. A flush or FUA is still needed. — [Redpanda/HN discussion summary](https://hn.nuxt.dev/item/48047280)
- Redpanda's log segment writers use DMA (O_DIRECT) to bypass the page cache for reads and writes. Redpanda reported "a factor of 10X" difference between sparse-file concurrent writes followed by fdatasync() and an O_DIRECT handle with forced flushes, which it attributes to the number of disk-controller flushes the OS issues. — [Redpanda: Autotune series part 1 – storage](https://www.redpanda.com/blog/autotune-series-part-1-storage)
- Redpanda credits O_DIRECT and io_uring, along with thread-per-core execution and CPU pinning, as Linux interfaces it uses directly. It aligns its cache with the filesystem to keep flushes efficient. — [What makes Redpanda fast?](https://www.redpanda.com/blog/what-makes-redpanda-fast); [Thread-per-core buffer management (CNCF repost, Jan 2021)](https://www.cncf.io/blog/2021/01/21/thread-per-core-buffer-management-for-a-modern-kafka-api-storage-system/)
- Redpanda's "write caching" mode (2024) relaxes how often broker state is synced with fdatasync(), trading durability for up to ~90% faster workloads. — [Redpanda: write caching benchmark](https://redpanda.com/blog/write-caching-performance-benchmark)
- `seastar::file_output_stream()` cannot flush unfinished (partial) pages. This is an example of the double-buffering/partial-page problem with O_DIRECT appends. — [search summary of Seastar discussion](https://hacklines.com/en/editions/287021/click) (secondary; low confidence)
- TigerBeetle manages its own page cache, writes with O_DIRECT, and can use a raw block device. Its design doc says Direct I/O is used to avoid kernel page-cache coherency bugs after an EIO fsync error. It adopts an explicit storage fault model (citing fsyncgate and Protocol-Aware Recovery) and repairs local corruption from cluster replicas. — [TigerBeetle docs: Safety](https://docs.tigerbeetle.com/concepts/safety/); [TigerBeetle DESIGN.md (mirror)](https://github.com/tbg/tigerbeetle/blob/main/docs/DESIGN.md)
- TigerBeetle has noted it had not yet verified whether cloud VMs honor O_DIRECT for NVMe or hide EIO faults behind their own caching. — [TigerBeetle docs: cloud](https://docs.tigerbeetle.com/about/internals/cloud)
- ScyllaDB (Seastar) published a July 2026 post on an "asymmetric io_uring backend" for Seastar, which shows Seastar's io_uring backend is still under active development. — [ScyllaDB blog 2026-07-22](https://www.scylladb.com/2026/07/22/asymmetric-io_uring-backend-seastar/) (content not reviewed in detail)
- Ceph BlueStore uses O_DIRECT with frequent flushes, and Ceph recommends PLP enterprise drives for that reason. — [datacenterdisk Q&A](https://datacenterdisk.com/questions/ceph-slow-with-consumer-ssds) (secondary)

### Inferences
- Double buffering for a group-commit WAL with O_DIRECT: the last partial 4 KiB block of each batch must be rewritten on the next commit (read-modify-write in memory). Alternatively, pad every batch to a block boundary, which wastes on average half a block per commit (2 KiB of 3-30 KB batches is about 7-60% overhead at the small end). Seastar's partial-page limitation shows this is a real design cost.
- O_DIRECT + RWF_DSYNC on an **overwrite of already-written, preallocated blocks** is the path where the kernel can avoid a journal commit and send a single FUA write, or a write + FLUSH if the device lacks FUA. With appends or unwritten extents, the filesystem must still journal metadata, so O_DIRECT gains little. Pre-zeroing matters for O_DIRECT too.
- For precog's 3-30 KB batches on a VM (virtio), the gains of O_DIRECT over buffered pwrite + fdatasync into a pre-zeroed file are probably small: one less memcpy, no page-cache pollution, and possible FUA. Its main benefit is avoiding fsyncgate page-cache semantics. Measure on the target VM.
- Iggy uses io_uring via compio but my sources do not say it uses O_DIRECT (see section 3).

### Gaps
- I could not confirm from a primary source which ext4/XFS kernel versions turn an O_DIRECT|O_DSYNC overwrite into a REQ_FUA write rather than write + cache flush. I recall an XFS iomap change around Linux 4.18 (2018), but it is unverified here. Check the kernel's `iomap_dio_rw` / `IOMAP_DIO_WRITE_FUA` history.
- Whether virtio-blk / cloud hypervisors pass FUA through or emulate it with a flush was not found. TigerBeetle flags this as untested.
- I found no source on whether Apache Iggy uses O_DIRECT.

## 3. io_uring: linked WRITE+FSYNC, registered buffers, SQPOLL; Rust crates; containers/seccomp; kernel versions

### Takeaway
io_uring can express "write batch, then fdatasync" as one linked submission (`IOSQE_IO_LINK` + `IORING_OP_FSYNC` with `IORING_FSYNC_DATASYNC`). That removes syscalls and thread hops but does not make the device flush any faster. Precog's commit latency is dominated by the flush (100-300 µs), so gains are small. Meanwhile io_uring is **blocked by Docker's default seccomp profile**, disabled on Google's production servers and ChromeOS, and can be switched off by sysctl. The Rust crate landscape is uneven: `io-uring` (low-level) and `compio` are active, while `tokio-uring`, `glommio`, and `monoio` are stale.

### Cited Findings
**Kernel primitives**
- io_uring first shipped in Linux 5.1 (2019), with IORING_OP_FSYNC from the start. `IORING_FSYNC_DATASYNC` gives fdatasync semantics. `IOSQE_IO_LINK` (5.3) makes the next SQE start only after this one completes successfully. `IORING_OP_WRITE` (non-vectored) arrived in 5.6. `IORING_REGISTER_BUFFERS` with `IORING_OP_WRITE_FIXED` supports pre-registered buffers. SQPOLL is a kernel thread that polls the SQ (unprivileged since 5.11). — [io_uring_enter(2)](https://man7.org/linux/man-pages/man2/io_uring_enter.2.html); [io_uring_setup(2)](https://man7.org/linux/man-pages/man2/io_uring_setup.2.html); [io_uring_register(2)](https://man7.org/linux/man-pages/man2/io_uring_register.2.html)
- Linux 6.6 added the `kernel.io_uring_disabled` sysctl (0 = on, 1 = only processes in `kernel.io_uring_group`, 2 = off for everyone), so hosts can turn it off. — [kernel sysctl docs](https://docs.kernel.org/admin-guide/sysctl/kernel.html#io-uring-disabled)
- io_uring completions are not ordered by submission. Apache Iggy found that batched writes can complete out of order and corrupt on-disk state after a crash. They fixed it with `IOSQE_IO_LINK` chaining. — [Apache Iggy blog, 2026-02-27](https://iggy.apache.org/blogs/2026/02/27/thread-per-core-io_uring/)
- An io_uring write completion (without DSYNC/fsync) only means the data reached the page cache. A July 2025 blog post on async I/O and durability says acking on completion without a durable flush "might" leave data "sitting in kernel buffers, not yet written to stable storage". It reports about 10x throughput from batching io_uring WAL writes, but gives no hardware or absolute numbers and does not specify the flush mechanism. — [Tregunna, "Async I/O on Linux and durability", 2025-07-18](https://blog.canoozie.net/async-i-o-on-linux-and-durability)

**Security and container blocking**
- Docker's default seccomp profile blocks `io_uring_setup`, `io_uring_enter`, and `io_uring_register` (moby/moby#46762), citing container-escape vulnerabilities. Programs then fail with EPERM. moby/moby#47532 asks to re-allow it and is open as an enhancement. Workaround: a custom profile via `--security-opt seccomp=`. — [Docker docs: seccomp](https://docs.docker.com/engine/security/seccomp/); [moby#47532](https://github.com/moby/moby/issues/47532); [docker/docs PR #24449](https://github.com/docker/docs/pull/24449)
- Google (Security Blog, June 2023, "Learnings from kCTF VRP's 42 Linux kernel exploits"): about 60% of 2022 kCTF kernel exploit submissions targeted io_uring, with about $1M paid. Google disabled io_uring on ChromeOS and on its production servers, blocks it for Android apps via seccomp, and was investigating disabling it by default in GKE Autopilot. — [oss-security 2023-06-17](https://openwall.com/lists/oss-security/2023/06/17/2); [oss-sec 2023 q2/235](https://seclists.org/oss-sec/2023/q2/235); [Phoronix](https://mail.phoronix.com/news/Google-Restricting-IO_uring)
- TigerBeetle acknowledges io_uring's exploit history as the reason some systems disable it, and accepts the risk because it handles only trusted data. — [TigerBeetle DESIGN.md](https://github.com/tbg/tigerbeetle/blob/main/docs/DESIGN.md)
- In 2025, a proof-of-concept rootkit ("Curing") used io_uring to evade syscall-based security monitoring. — [Schneier, Apr 2025](https://www.schneier.com/blog/archives/2025/04/new-linux-rootkit.html)

**Rust crates (crates.io API, queried 2026-11-17)**

| Crate | Latest release (date) | Release cadence | License | Runtime model | Status notes |
|---|---|---|---|---|---|
| `io-uring` (tokio-rs) | 0.7.16 (2026-11-09) | 0.7.13 Jun 28, .14 Aug 11, .15 Sep 7, .16 Nov 9 2026 | MIT OR Apache-2.0 | Low-level, safe-ish SQE/CQE bindings; no executor. Good fit for a dedicated WAL writer thread. | Active; about 71.5M downloads |
| `tokio-uring` | 0.5.0 (2024-05-27) | prior 0.4.0 Nov 2022 | MIT | Single-threaded runtime on top of tokio, owned-buffer API | Stale: no release in about 2.5 years. Community thread asks about its status. |
| `compio` | 0.20.0-beta.1 (2026-09-29); stable 0.19.2 (2026-08-18) | 0.19.0 May 31, 0.19.1 Jun 14, 0.19.2 Aug 18 2026 | MIT | Thread-per-core, completion-based; io_uring on Linux, IOCP on Windows, polling fallback elsewhere; driver decoupled from executor | Active; Apache Iggy's choice |
| `glommio` (DataDog) | 0.9.0 (2024-03-25) | prior 0.8.0 Jan 2023 | Apache-2.0 OR MIT | Thread-per-core, io_uring-only, Seastar-like, has DMA (O_DIRECT) file API | Iggy (Feb 2026): "pretty much unmaintained at this point". An ecosystem index showed last push 2024-06-20. |
| `monoio` (ByteDance) | 0.2.4 (2024-08-20) | 0.2.3 Mar 2024 | MIT OR Apache-2.0 | Thread-per-core, io_uring + epoll fallback | Iggy: "doesn't appear to be very actively maintained", lags in io_uring feature parity, still gets patches |

Sources: [crates.io io-uring](https://crates.io/crates/io-uring), [tokio-uring](https://crates.io/crates/tokio-uring), [compio](https://crates.io/crates/compio), [glommio](https://crates.io/crates/glommio), [monoio](https://crates.io/crates/monoio); [Iggy blog 2026-02-27](https://iggy.apache.org/blogs/2026/02/27/thread-per-core-io_uring/); [users.rust-lang.org "Status of tokio_uring?"](https://users.rust-lang.org/t/status-of-tokio-uring/114481); [ecosyste.ms glommio](https://awesome.ecosyste.ms/projects/github.com%2Fdatadog%2Fglommio)

- Apache Iggy (incubating) moved from Tokio to thread-per-core with compio. Each shard runs its own compio runtime with an io_uring ring (default capacity 4096), pinned to a CPU. On macOS compio uses its polling driver plus a blocking pool for files. They chose compio for broad io_uring coverage, active maintenance (patches merged "within hours"), and the decoupled driver. They first prototyped on monoio. — [Iggy architecture docs](https://iggy.apache.org/docs/introduction/architecture); [Iggy blog](https://iggy.apache.org/blogs/2026/02/27/thread-per-core-io_uring/); [Iggy 0.6.0 release, 2025-12-09](https://iggy.apache.org/blogs/2025/12/09/release-0.6.0)
- Problems Iggy reported with compio: every submitted op is boxed, so one heap allocation per request (they judged it acceptable with mimalloc). The POSIX-shaped `File`/`TcpStream` abstractions make request chaining, registered buffers, and similar features hard to reach. There is no work stealing, so hot shards need manual balancing. RefCell borrows held across `.await` caused panics. — [Iggy blog](https://iggy.apache.org/blogs/2026/02/27/thread-per-core-io_uring/)
- Iggy benchmark numbers, per node, hardware not stated in the extracted text. Fsync-per-batch mode, 16×16 producers, 40 GB: v0.5.0 (Tokio) 843 MB/s, P99 19.72 ms; v0.7.0 (compio/io_uring) 992 MB/s, P99 13.04 ms. 32×32: 931 → 1,102 MB/s, P99 37.09 → 23.74 ms. Non-fsync tail latency P9999 fell from 34-86 ms to 6.5-11.8 ms. — [Iggy blog](https://iggy.apache.org/blogs/2026/02/27/thread-per-core-io_uring/)
- Iggy also noted that Tokio's blocking pool for file I/O can grow to 512 threads, which it found unsuitable. — [Iggy blog](https://iggy.apache.org/blogs/2026/02/27/thread-per-core-io_uring/)

### Inferences
- Precog has one dedicated WAL writer thread doing group commit, so the main io_uring benefit (many concurrent in-flight I/Os without threads) mostly doesn't apply. A blocking writer thread doing `pwrite` + `fdatasync` issues 2 syscalls per batch, about 1-2 µs of overhead against a flush of 100-300 µs. io_uring could let the writer overlap "flush batch N" with "write batch N+1". But fdatasync flushes the whole file, so batch N+1's write can't be acked until the next flush anyway. Pipelining yields at most about one extra batch in flight.
- If precog uses io_uring, the low-level `io-uring` crate on the dedicated writer thread is the lowest-risk option: active, permissive license, no runtime lock-in. It needs a fallback path (`pwrite`+`fdatasync`) for EPERM/ENOSYS from seccomp, gVisor, or `io_uring_disabled`. Choosing glommio, monoio, or tokio-uring as a foundation carries maintenance risk based on release dates.
- Always link the WRITE → FSYNC chain with `IOSQE_IO_LINK`, or wait for the write CQE before submitting the fsync. Without ordering, the fsync can run before the write and the ack would be unsafe.

### Gaps
- gVisor's io_uring support: I found no current primary source. As I recall, gVisor implements only a limited subset, but this is unverified. Treat as a deployment risk to test.
- Last-commit dates for the GitHub repos (glommio, monoio, tokio-uring) could not be fetched in this session. Only crates.io release dates are confirmed.
- Whether Tokio's own experimental io_uring file backend (tokio_unstable) is production-ready in Nov 2026 was not researched.
- No published microbenchmark found comparing io_uring linked write+fdatasync against blocking pwrite+fdatasync latency for a single-writer WAL.

## 4. sync_file_range: not a durability primitive

### Takeaway
`sync_file_range` only starts or waits for writeback of dirty pages in a range. It writes no metadata and issues no device cache flush, so it gives no crash guarantee. It is useful only to smooth writeback (RocksDB `bytes_per_sync`-style) before a real fdatasync.

### Cited Findings
- The man page says sync_file_range "is extremely dangerous and should not be used in portable programs". None of its operations write out the file's metadata, and it gives no guarantee that data is safe after a crash, because it doesn't flush the disk write cache or handle allocation metadata, for example of fallocated or sparse regions. — [sync_file_range(2) man page](https://man7.org/linux/man-pages/man2/sync_file_range.2.html)

### Inferences
- For precog, sync_file_range might be used on sealed-segment writers (start writeback early so the final fdatasync is short). Never use it on the commit path.

### Gaps
- No precog-relevant benchmark of sync_file_range smoothing was found.

## 5. mmap-based logs

### Takeaway
Most databases avoid mmap for writes. The kernel decides when dirty pages are written back, `msync` is the only durability control, errors surface as SIGBUS rather than return codes (for example disk full on a sparse mapping, or I/O error), and page-fault-driven I/O stalls unpredictably.

### Cited Findings
- Crotty, Leis & Pavlo (CIDR 2022), "Are You Sure You Want to Use MMAP in Your Database Management System?", list transactional-safety problems (the OS can flush dirty pages at any time, so no write ordering control), I/O stalls on page faults, error handling via SIGBUS instead of error codes, and TLB-shootdown performance problems at scale. — [CIDR 2022 paper](https://db.cs.cmu.edu/mmap-cidr2022/)
- mmap(2): access to a mapped region beyond the end of the file raises SIGBUS. msync(2) with `MS_SYNC` is required to flush changes to a shared file mapping. — [mmap(2)](https://man7.org/linux/man-pages/man2/mmap.2.html); [msync(2)](https://man7.org/linux/man-pages/man2/msync.2.html)

### Inferences
- mmap is reasonable for **reading** sealed, immutable segments (zero-copy, page cache shared). For the WAL write path it adds SIGBUS handling and loses the explicit error returns that section 6 says you need.

### Gaps
- No primary source fetched for "SIGBUS on ENOSPC with a sparse shared mapping". It is widely reported, and the CIDR paper covers SIGBUS error handling in general.

## 6. Durability gotchas

### 6a. fsyncgate (PostgreSQL, 2018)

#### Takeaway
On Linux, a failed writeback marks pages clean (or drops them), and the error is reported **once**. Retrying fsync can "succeed" after data was lost. The only safe reaction to an fsync/fdatasync error on a WAL is to treat the in-memory state as untrustworthy: crash and recover from the durable log (or, for precog, from peers).

#### Cited Findings
- PostgreSQL assumed a successful fsync means all previously written data is on disk. In 2018 (the "fsyncgate" mailing list thread, opened by Craig Ringer) it learned this is false on Linux after a writeback error. — [PostgreSQL wiki: Fsync Errors](https://wiki.postgresql.org/wiki/Fsync_Errors); [fsyncgate thread](https://www.postgresql.org/message-id/flat/CAMsr%2BYHh%2B5Oq4xziwwoEfhoTZgr07vdGG%2Bhu%3D1adXx59aTeaoQ%40mail.gmail.com); [LWN "PostgreSQL's fsync() surprise", Apr 2018](https://lwn.net/Articles/752063/)
- Kernel history per the PG wiki:
  - Before Linux 4.13, errors can be lost in various ways, and a retried fsync can falsely report success.
  - Linux 4.13 added errseq_t-based writeback error reporting ("fs: new infrastructure for writeback error handling and reporting"), with ext4 converted to it.
  - In 4.13-4.15, fsync reports only errors that occurred after this fd was opened.
  - From 4.14/4.16, the counter is initialized so that someone sees the inode's first error even after close/reopen, but "you still only get the error once (so retrying fsync() is not OK)".
  - Buffers are still marked clean and may be discarded, so an older page version may be read back. — [PostgreSQL wiki: Fsync Errors](https://wiki.postgresql.org/wiki/Fsync_Errors); [LWN "Improved block-layer error handling" (2017)](https://lwn.net/Articles/724307/)
- PostgreSQL's fix: PANIC on fsync() failure (PG 12, backpatched to 11, 10, 9.6, 9.5, 9.4). Credited to Thomas Munro, Andres Freund, Robert Haas, and Craig Ringer. — [PostgreSQL wiki](https://wiki.postgresql.org/wiki/Fsync_Errors)
- Behavior on other OSes: FreeBSD keeps buffers dirty, so a retry works. macOS/NetBSD/OpenBSD invalidate buffers, so a retry may falsely succeed. — [PostgreSQL wiki](https://wiki.postgresql.org/wiki/Fsync_Errors)
- Rebello et al., USENIX ATC 2020, "Can Applications Recover from fsync Failures?", studied ext4/XFS/Btrfs and applications (including PostgreSQL, LMDB, LevelDB, SQLite, Redis) and found that file systems handle fsync failures inconsistently and that applications mishandle them. — [ATC'20 paper (PDF)](https://www.usenix.org/system/files/atc20-rebello.pdf)
- TigerBeetle cites fsyncgate as a reason to use O_DIRECT (no page cache to go stale) and an explicit storage-fault model. — [TigerBeetle safety docs](https://docs.tigerbeetle.com/concepts/safety/)

#### Inferences (implications for precog's WAL)
- On any `sync_data()`/fdatasync error (EIO, ENOSPC, EDQUOT): never retry and ack. Fail every pending append in the batch. Stop writing to that log. Crash the node or fence it, then restart, recover from the last verified record on disk, and re-replicate missing data from mesh peers. Do not trust page-cache contents, because reads may return stale data.
- Keep the WAL fd open for the life of the segment. Error reporting is tied to the file (errseq_t per mapping), but on old kernels a closed-and-reopened fd could miss errors.
- ENOSPC can surface at fdatasync time with buffered writes into sparse space. Pre-allocating written space removes most of this risk.

### 6b. fdatasync vs fsync, write cache flush, lying disks, PLP

#### Takeaway
fdatasync skips metadata not needed to read the data back, but a file-size change or extent conversion forces an ext4/XFS journal commit. Flush cost depends mostly on the device. Enterprise NVMe with power-loss protection (PLP) completes flushes in about 1-12 µs. Consumer NVMe without PLP takes about 0.4-3 ms. Cloud network disks fall in between, at tens of µs to ms.

#### Cited Findings
- Mark Callaghan (Small Datum, 2026-01-07) ran fio with O_DIRECT, 1 job, and 5-minute runs, measuring sync latency in µs at a 16 KB write size (fsync / fdatasync):
  - Crucial T500 (consumer, ext4): 891 / 447
  - Samsung 990 Pro (consumer, no PLP, ext4): 2,974 / 2,783
  - Intel/Solidigm D7-P5520 (enterprise, SW RAID1, ext4): 12.4 / 9.8
  - Samsung PM9A3 (enterprise PLP, ext4): 1.6 / 0.7
  - GCP local NVMe (RAID0): ext4 56 / 28, xfs 39.5 / 29
  - GCP Hyperdisk Balanced (50k IOPS): ext4 738 / 46.8, xfs 635 / 46.0

  At 2 MB the 990 Pro reached 3.5-5.4 ms (up to 10.8 ms fsync in one run). Note the big fsync-vs-fdatasync gap on Hyperdisk: metadata commits are expensive on network disks. — [Small Datum: SSDs, power loss protection and fsync latency](https://smalldatum.blogspot.com/2026/01/ssds-power-loss-protection-and-fsync.html)
- A secondary write-up reports a consumer NVMe dropping from about 334k to about 2k IOPS under ZFS sync=always, and says PLP drives commonly reach 15k-60k+ fsync/s. — [computingforgeeks](https://computingforgeeks.com/nvme-vs-sata-ssd-proxmox-datastore/) (secondary, unverified)
- AWS: io2 Block Express is designed for average latency under 500 µs for 16 KiB I/O. gp3 is described as "single digit millisecond" latency, and one AWS test observed 0.478 ms average at 40k IOPS on gp3. — [AWS Storage Blog: io2 Block Express for databases](https://aws.amazon.com/blogs/storage/achieve-higher-database-performance-using-amazon-ebs-io2-block-express-volumes); [AWS: PostgreSQL on io2 Block Express](https://aws.amazon.com/blogs/storage/running-i-o-intensive-workloads-on-postgresql-with-amazon-ebs-io2-block-express)
- A ClickHouse/POSETTE talk recap measured PostgreSQL WAL flush at commit of about 11 ms on gp3 EBS (3,000 IOPS baseline) versus about 1.5 ms on instance-store NVMe. — [ClickHouse blog: Postgres isn't slow, your storage is](https://clickhouse.com/blog/posette-talk-recap-postgres-isnt-slow-your-storage-is)
- Percona found io1/io2 EBS latencies more consistent than gp2/gp3, especially at higher thread counts. — [Percona: performance of various EBS storage types](https://www.percona.com/blog/performance-of-various-ebs-storage-types-in-aws/)
- An etcd-on-EBS case measured p99 fdatasync of about 2,671 µs with fio (volume type unclear) before an upgrade. — [besthub.dev etcd/EBS article](https://www.besthub.dev/articles/how-upgrading-ebs-volumes-boosted-etcd-write-performance-by-30-0d1850a8da3a) (secondary)
- fsync docs: on drives with a volatile write cache, fsync/fdatasync must also flush the cache. The kernel issues a cache-flush (or uses FUA) when the device advertises a volatile cache. — [fsync(2)](https://man7.org/linux/man-pages/man2/fsync.2.html)

#### Inferences
- Precog's VM numbers (123 µs pre-zeroed, 297 µs appending) look like a virtio device whose flush is cheap-ish but not PLP-cheap. The 174 µs delta is roughly one extra journal commit (journal write + flush).
- On PLP enterprise NVMe the device flush is about 1-12 µs, so the **filesystem metadata commit becomes the dominant cost**. Avoiding size changes and unwritten extents matters even more there, in relative terms.
- On network block storage (EBS gp3 / Hyperdisk), expect 0.5-11 ms per durable commit, with fsync much worse than fdatasync. Group-commit batch size will grow automatically under load.
- Precog's design should not trust a single node's flush for durability against lying devices. Replication across the mesh (ack after K nodes' fdatasync) is the mitigation, as TigerBeetle argues.

#### Gaps
- No primary measurement found for Intel Optane (P4800X/P5800X) fsync/FUA latency. Optane is discontinued anyway.
- I found no current primary source that catalogues "disks that lie about flush" by model. Historically Brad Fitzpatrick's `diskchecker.pl` (2005) was the tool. Recommend running a power-pull or diskchecker test on chosen hardware.
- I found no published fdatasync latency figure for GCP Persistent Disk (pd-ssd/pd-balanced), as distinct from Hyperdisk.

### 6c. Directory fsync after create/rename

#### Takeaway
Creating, renaming, or unlinking a segment file is a directory change. Durably persisting it requires fsync on the **directory fd**, not just the file.

#### Cited Findings
- "Calling fsync() does not necessarily ensure that the entry in the directory containing the file has also reached disk. For that an explicit fsync() on a file descriptor for the directory is also needed." — [fsync(2)](https://man7.org/linux/man-pages/man2/fsync.2.html)
- ALICE found that the common "persist data, then rename" heuristic fixes only a small minority (3) of the discovered vulnerabilities. The order in which file systems persist operations varies. For example, ext3 data=ordered persists an append to file A before a later rename of file B, but ext4 data=ordered does not (unless a special option is enabled). — [Pillai et al., OSDI 2014 (PDF)](https://pages.cs.wisc.edu/~remzi/Classes/739/Fall2016/Papers/alice-osdi14.pdf); [The Morning Paper summary](https://blog.acolyer.org/2016/02/11/fs-not-equal/)

#### Inferences
- Precog segment lifecycle:
  1. Create `seg.tmp` and zero-fill it.
  2. fdatasync the file.
  3. Rename it to `seg-<id>`.
  4. fsync the directory.
  5. Start using it.

  For a recycled file, renaming and then fsyncing the dir is enough before reuse. Before acknowledging any record written into a new segment, make sure the directory entry is durable. Otherwise a crash can lose the whole file even though fdatasync succeeded. In Rust, `File::open(dir)?.sync_all()` works on Linux.

### 6d. Torn writes, sector atomicity, checksums; ALICE findings

#### Takeaway
Linux and filesystems promise no atomicity for multi-sector writes. A crash can leave any prefix or subset of a 3-30 KB batch on disk. Every record or batch needs a length + CRC (plus an LSN/generation), and recovery must truncate at the first invalid record. ALICE showed that even mature applications get crash consistency wrong.

#### Cited Findings
- ALICE (Pillai et al., OSDI 2014) analysed 11 applications, including LevelDB, SQLite, Git, ZooKeeper, HDF5, and QEMU, and found 60 vulnerabilities. Many led to silent corruption, durability loss, or an inaccessible application. About half occur on current file systems (ext3/ext4/btrfs). Its BOB tool showed persistence properties (atomicity of appends, ordering of operations) vary widely across six Linux file systems and their modes. — [UW ADSL ALICE page](https://research.cs.wisc.edu/adsl/Publications/alice-osdi14.html); [paper PDF](https://cs.uwaterloo.ca/~alkiswan/papers/alice-osdi14.pdf); [Murat Demirbas summary](https://muratbuffalo.blogspot.com/2015/04/all-file-systems-are-not-created-equal.html)
- A secondary review counts the 60 as 5 silent failures, 12 durability losses, 25 inaccessible-application cases, and 17 error-on-access. These sum to 59, so treat the breakdown as approximate. — [The Morning Paper](https://blog.acolyer.org/2016/02/11/fs-not-equal/)
- A key ALICE lesson is that appends are not atomic on many filesystem configs. Garbage or zeroes can appear in the extended region after a crash. Applications must checksum appended data. — [ALICE paper](https://pages.cs.wisc.edu/~remzi/Classes/739/Fall2016/Papers/alice-osdi14.pdf)
- Real WAL designs checksum every entry. The 2025 io_uring WAL design puts CRC32 over key+value in each completion entry. TigerBeetle checksums all data and tests under heavy injected corruption. — [Tregunna 2025](https://blog.canoozie.net/async-i-o-on-linux-and-durability); [TigerBeetle safety](https://docs.tigerbeetle.com/concepts/safety/)

#### Inferences
- Per-batch framing for precog: `[magic | segment_generation | lsn | len | crc32c(header+payload)] payload`. Pad to 4 KiB only if you go O_DIRECT. On recovery, scan forward and stop at the first record whose CRC fails, whose LSN is not exactly the expected next value, or whose length is zero (pre-zeroed tail). Truncate or zero everything after it.
- Distinguish a **torn tail** (expected after a crash, truncate silently) from **mid-log corruption** (valid records after an invalid one). Mid-log corruption means media corruption and should trigger repair from peers, as TigerBeetle does, not a silent truncate.
- Never ack until both the batch and every earlier batch are covered by a successful fdatasync. With group commit this holds naturally.

#### Gaps
- No primary source fetched on NVMe atomic write unit (AWUN/AWUPF) values or the Linux 6.11+ `RWF_ATOMIC` atomic-write API. Both are relevant if precog ever wants 4 KiB+ untorn writes. Worth a follow-up.

### 6e. Typical fsync/FUA latencies (summary)

#### Takeaway
Order of magnitude per durable 16 KB commit:
- Enterprise PLP NVMe: about 1-12 µs device flush.
- GCP local NVMe: about 30-55 µs.
- Cloud network disks: about 46 µs fdatasync / 640-740 µs fsync on Hyperdisk Balanced, under 500 µs average I/O on io2 Block Express, about 0.5-11 ms on gp3.
- Consumer NVMe without PLP: about 0.45-3 ms.

The precog test VM (123-297 µs) sits in the cloud/virtio middle.

#### Cited Findings
- See 6b: [Small Datum 2026-01-07](https://smalldatum.blogspot.com/2026/01/ssds-power-loss-protection-and-fsync.html); [AWS io2 BE](https://aws.amazon.com/blogs/storage/achieve-higher-database-performance-using-amazon-ebs-io2-block-express-volumes); [ClickHouse/POSETTE recap](https://clickhouse.com/blog/posette-talk-recap-postgres-isnt-slow-your-storage-is).
- Iggy's fsync-per-batch mode at about 1 GB/s showed P99 of 13-24 ms (v0.7.0), hardware not given, so fsync-heavy brokers see ms-scale tails under load. — [Iggy blog](https://iggy.apache.org/blogs/2026/02/27/thread-per-core-io_uring/)

#### Gaps
- Optane, GCP PD-SSD, and Azure Premium SSD v2 fsync numbers were not found in primary sources.
- None of the cited measurements are on virtio-blk with a known host cache mode (cache=none / writeback / directsync). Precog's VM numbers depend on the hypervisor's cache mode, and whether the host honors flushes is unknown.

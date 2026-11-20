# Where dormant streams' summaries live (docs/S2-PLAN.md C7)

A dormant stream is a ~120 B summary on disk. The store needs: checkpoints that write the changed
streams' summaries durably (one commit per retention pass), a point read to wake a stream, a
point read that misses for every new stream, a reopen that does not walk every stream, and little
RAM. Two probes, on this 4-vCPU cloud container (one run each):

1. `storage-bench --summaries N` (`crates/storage-bench`): the store alone. Loads N summaries in
   commits of 10k, then 200 commits of 1k changed ones, drops caches, reopens, and times 100k
   wakes (hits) and 100k new streams (misses), one get at a time. `results/dormant-store.jsonl`.
2. `crates/mesh/examples/streams.rs`: the whole node, with redb (this PR) and with the summary
   store swapped for fjall (a POC of `summary::Db` on fjall, not merged), 1M streams, one record
   each, 16 MiB segments, a retention pass every 100k streams.

## The store alone

| backend | streams | load/s | write amp (load) | 1k-checkpoint p50 / p99 | written per 1k-checkpoint | reopen | wake p50 / p99 (cold) | miss p50 / p99 | RSS after | disk |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| redb 4.6 | 1M | 66k | 24x | 29 / 37 ms | 5.8 MB | 1.6 ms | 3.3 / 74 µs | 2.2 / 39 µs | 256 MiB | 239 MiB |
| LMDB (heed) | 1M | 86k | 25x | 31 / 43 ms | 5.8 MB | 1.6 ms | 1.1 / 93 µs | 1.1 / 1.9 µs | 316 MiB | 289 MiB |
| fjall 3.1 | 1M | 419k | 1.9x | 2.8 / 6.2 ms | 0.40 MB | 856 ms | 6.8 / 77 µs | 1.7 / 3.3 µs | 180 MiB | 218 MiB |
| RocksDB | 1M | 422k | 1.9x | 3.1 / 3.8 ms | 0.14 MB | 890 ms | 9.0 / 116 µs | 12 / 33 µs | 118 MiB | 165 MiB |
| SQLite (WAL) | 1M | 38k | 57x | 16 / 149 ms | 7.4 MB | 1.4 ms | 5.7 / 93 µs | 4.0 / 15 µs | 18 MiB | 158 MiB |
| log + index in RAM | 1M | 1.6M | 1.1x | 1.1 / 2.3 ms | 0.15 MB | 588 ms (replay) | 0.7 / 1.3 µs | 0.09 / 0.4 µs | 128 MiB | 169 MiB |
| redb 4.6 | 10M | 35k | 38x | 49 / 65 ms | 7.9 MB | 3.0 ms | 42 / 109 µs | 39 / 85 µs | 1,081 MiB | 2,069 MiB |
| LMDB (heed) | 10M | 41k | 37x | 43 / 73 ms | 7.8 MB | 2.6 ms | 2.4 / 207 µs | 2.0 / 47 µs | 2,223 MiB | 2,248 MiB |
| fjall 3.1 | 10M | 280k | 4.2x | 3.3 / 73 ms | 1.5 MB | 17 ms | 784 / 2,351 µs | 453 / 1,325 µs | 302 MiB | 732 MiB |
| RocksDB | 10M | 245k | 4.3x | 3.3 / 70 ms | 1.1 MB | 970 ms | 21 / 359 µs | 13 / 102 µs | 135 MiB | 1,608 MiB |
| log + index in RAM | 10M | 1.4M | 1.1x | 1.2 / 1.8 ms | 0.16 MB | 5,528 ms (replay) | 0.9 / 1.5 µs | 0.16 / 0.5 µs | 614 MiB | 1,440 MiB |

RSS of the B-trees is their memory map (page cache), not heap. The bench gives redb a 1 GiB cache;
the node gives it 64 MiB (`summary_cache_bytes`). fjall's 10M wakes are its first reads after a
cold reopen: it had not loaded its filters and indexes yet.

## The whole node, 1M streams

| | create/s | of which retention passes | heap after creating | restart | heap after restart |
|---|---:|---:|---:|---:|---:|
| main (4c7cd26), no dormancy | 47–49k | n/a | 2,181 MiB (2.3 KB/stream) | 4.2–4.3 s | 2,021 MiB |
| this PR (redb), streams idle at once | 24–29k | 9.3 s | 313–317 MiB | 0.33–0.36 s | 117 MiB |
| POC: summaries in fjall | 26–29k | not measured | 364 MiB | 2.7–3.0 s | 115 MiB |
| this PR (redb), default 10 min idle (no evictions during the run) | 41k | n/a | 503 MiB | 0.33 s | 117 MiB |

Under callgrind (200k streams), redb's B-tree inserts are 26% of the node's instructions, and
writing the checkpoint is 24%.

## What it says

- **Swapping the store buys little end to end.** fjall writes 4–14x fewer bytes per checkpoint,
  yet the node creates streams at the same rate and restarts 8x slower (its journal is replayed).
- **The B-trees' cost is write amplification**: ~6–8 KB written per summary changed. That matters
  when many streams change between passes: 100k changed streams per 10 s pass are ~80 MB/s of writes.
- **A log with its index in RAM is fastest at everything but RAM and restart** (~60 B per stream
  and a replay of every summary), which are what C7 is for.
- **RocksDB balances best** (cheap checkpoints, bloom-filtered misses), but it is C++, takes minutes
  to build and needs libclang.

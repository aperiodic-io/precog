# Formal verification

What is checked, with which tool, to what bound, and what is left out. Every check runs in CI:
the `formal` job on each pull request, bigger bounds in `nightly.yml`.

| Part | Tool | Why this tool | Where |
|---|---|---|---|
| Crash recovery of one segment, group commit, write caching | TLA+ / TLC | Exhaustive interleavings of writes, writeback in any order, power loss and media damage; small state spaces, no infrastructure | `spec/storage/SegmentLog.tla` |
| Segment roll, directory fsyncs, the two-slot manifest | TLA+ / TLC | A power loss between any two steps of a multi-file protocol | `spec/storage/SegmentRoll.tla` |
| Decoders of records, manifest slots and index files | Kani | Bounded proofs over every byte pattern, at the code itself | `#[cfg(kani)] mod proofs` in `crates/storage/src/{record,log}.rs` |
| Leaderless appends at k of n, failure domains, retries, conflicts, repair, dedup, write caching, `Client::end` | TLA+ / TLC | The protocol's failure modes are interleavings of messages, power losses and disk losses across nodes | `spec/mesh/Replication.tla` |
| Reader-group positions and compacted streams across nodes; cursor reads with failover | TLA+ / TLC | Commits and batches reach nodes in any order | `spec/consumers/Positions.tla` |
| Wire codecs: batches (every format), acks, append and replicate bodies, log keys, compacted-stream snapshots | proptest | The bytes come from the network or the disk; Kani does not finish them (below) | `mod props` in `crates/mesh/src/{proto,node}.rs` |

Run them:

```sh
spec/check.sh                 # TLC, the PR bounds (downloads tla2tools.jar 1.7.4, Java 11+)
spec/check.sh nightly         # also spec/*/Nightly_*.cfg
cargo install --locked kani-verifier --version 0.68.0 && cargo kani setup
cargo kani -p precog-storage -Z stubbing
```

A config whose first line is `\* expect: <Invariant>` must fail on that invariant: it keeps the
counterexample of a bug a fix closed, so the model is shown to catch it (`spec/check.sh` fails
if it stops failing).

## Storage: `spec/storage`

### `SegmentLog.tla`: one active segment

**Models.** `Log::append` (one write per group; without write caching an fsync per group before
the ack, one group at a time; with write caching an ack once written, the deferred flush outside
the log's lock while appends go on (`Log::begin_flush`, `Flush::sync`, `Log::end_flush`), and an
inline sync once unsynced plus in-flight bytes reach twice `flush_bytes`), the kernel writing dirty pages back in any order, a
power loss that keeps each unsynced record whole, torn or not at all, media damage to a
persisted record, and the recovery scan of `scan()` step for step: the forward scan with its LSN
and group checks, the search past the stop for a later group (fail closed) or the torn group's
own records (truncate), and the zeroing of every leftover record before the log goes on. A
recycled file starts with records of an older generation in any slot.

**Abstracts away.** Record sizes and the 8-byte search stride (every record start is a slot;
slot i holds LSN i), the read window (it covers the segment), CRC-32C (it catches every torn
or damaged record; the Kani proofs cover the decoder itself), and other segments (the roll is
`SegmentRoll.tla`).

**Invariants.**
- `Prefix`: the log below its end is exactly what was last written there: no record of an older
  generation, no leftover of a dropped group, no hole (a record damaged in place is refused on
  read).
- `AckedSurvive`: without write caching and without media damage, every acknowledged record
  survives any sequence of power losses.
- `NoFalseAlarm`: recovery refuses to open only after real media damage.
- `FailClosed`: media damage is refused, or costs at most the newest acknowledged group (damage
  there followed only by writes the power loss discarded is indistinguishable from a torn tail;
  the replicas and repair restore it).

**Bounds.**

| Config | Segment | Group | Crashes | Other | Distinct states |
|---|---|---|---|---|---|
| `MC_sync` | 5 records | ≤ 2 | 2 | media faults | 168,792 |
| `MC_caching` | 6 | ≤ 2 | 1 | flush at 2, bound 6 (`2 × flush + group`) | 579,793 |
| `Nightly_sync` | 6 | ≤ 3 | 2 | media faults | 1,502,239 |
| `Nightly_caching` | 6 | ≤ 2 | 2 | flush at 2, bound 6, media faults (`Prefix`) | 33,850,249 |
| `MC_caching_onebound` (expect) | 6 | ≤ 2 | 1 | bound 4 (`flush + group`, before #16): `NoFalseAlarm` fails | |

**Found.** Under write caching several groups are unsynced at once and the kernel writes them
back in any order, so a power loss can keep a later group and lose an earlier one. The scan
read that as damage followed by a later group and refused to open: a node of live-ingestion's
configuration would not restart after a power loss (`MC_caching_unfixed`). Fixed: at most
`Log::unsynced_bound` (twice `flush_bytes`, what an outstanding flush covers plus what landed
meanwhile, plus one group) can be unsynced, so with write caching a hole followed only by
records within that bound is the unsynced tail and is dropped as a whole (repair refills it from
the replicas); a hole followed by more than that is still damage and fails closed. Tests:
`a_power_loss_under_write_caching_drops_the_unsynced_tail_whatever_reached_disk` (failed first)
and `under_write_caching_damage_before_more_than_the_unsynced_bound_still_fails_closed`; the
bound to the byte: `under_write_caching_the_unsynced_bound_is_exact_to_the_last_record_written`.
In slots the worst case is one slot under the bound (the inline-sync check is strict); a bound
one slot shorter still holds, two shorter fails.

### `SegmentRoll.tla`: a segment roll and the restart after it

**Models.** `Log::roll`: rename the spare into place, fsync the directory, write and sync the
new header, write and sync the next manifest slot, switch appends to the new segment, write the
sealed segment's index (tmp file, rename, directory fsync); a power loss before any step, a torn
manifest slot write, and `Log::open` on what survives: the newest valid slot, every listed
segment present with a matching header, a successor the manifest missed adopted when its header
is durable.

**Abstracts away.** Records (counts; `SegmentLog.tla` checks the scan), spare preallocation,
retention and recycling.

**Invariants.** `OpensAfterAnyCrash` (a power loss at any step never stops the log from
opening) and `AckedSurvive` (the open serves every acknowledged record, the new segment's
included). `MC_roll_nodirsync` shows the directory fsync after the rename is needed: without
it the manifest can name a segment whose directory entry was lost. Exhaustive (27 states).

### Kani proofs in `crates/storage`

| Harness | Property | Bound |
|---|---|---|
| `record::proofs::decode_is_total_and_bounded` | `decode` never panics; an accepted record lies inside the buffer, key and payload exactly fill its length, its generation is the one asked for, `next` is 8-aligned | 72-byte buffers, every offset 0..=9, any bytes and checksum |
| `record::proofs::padded_record_roundtrips` | `decode(encode(x)) = x` field for field, `Invalid` under any other generation | 2-byte key, 3-byte payload, any field values |
| `log::proofs::manifest_decode_never_panics` | a manifest slot of any bytes decodes or is refused | 0..=101 bytes |
| `log::proofs::index_decode_never_panics` | an index file of any bytes decodes or is refused; an accepted one has the entries it declares | 0..=116 bytes: one entry with an 8-byte key, or two with empty keys |

CRC-32C is stubbed (unconstrained, or constant for the round trip), so the proofs also cover a
torn or foreign write whose checksum happens to match.

**Found.** `Manifest::decode` indexed its body before checking its length: a slot whose body
is shorter than its fixed fields under a matching checksum panicked the open instead of falling
back to the other slot (`manifest_decode_never_panics`; test
`a_manifest_body_too_short_for_its_fields_is_refused_not_a_panic`).

## Replication and dedup: `spec/mesh/Replication.tla`

**Models.** `Node::append` on any node: `begin_local` (a new batch's keyed events checked
against the node's dedup index and the batch queued to the log under one lock; a retry or a
conflict goes to `store_local`), the WAL thread committing in queue order (fsync, or with write
caching a write), replication of the kept bytes to one peer per other failure domain, the
replica's `store_local` (Ok/Duplicate once durable, Conflict, recording the origin's kept keys),
lost requests and replies, the ack once `copies` distinct domains confirmed with the origin's
own durable copy among them, client retries of the same bytes on any node, repair copying a
durable batch a peer lacks, the deferred fsync, power losses (losing what was not durable, with
write caching what was not fsynced; the dedup index rebuilt from the log on restart), and disks
lost for good (the node comes back empty).

**Abstracts away.** Batch contents (an identity, the dedup keys of its events, a content
variant for a conflicting reuse), sequence ranges and partial overlaps (the store's overlap
rules are unit-tested), the dedup window and its cap (keys are remembered for the whole run:
expiry only weakens dedup, never durability), event payloads, retention, live subscriptions.
Storing at a replica and replying are one step; a power loss between them is a lost reply.

**Properties.**
- `AckedSurvive`: with at most `copies - 1` faults (disk losses, and power losses under write
  caching), every acknowledged batch has a durable copy.
- `AckedInCopiesDomains`: without faults, an acknowledged batch is durable in `copies` domains.
- `NoConflictingAcks`: a conflicting reuse of an identity is never acknowledged next to the
  original while no node has lost a copy (two ack sets of `copies > n/2` nodes share a node,
  and it refuses the second variant). After a lost disk the empty node can accept the second
  variant, and both can be acknowledged (`MC_conflict` without the premise). Identity reuse is
  a writer bug; a node that later finds itself in the minority gives its bytes up
  (`yield_to_majority`, #19, not modelled), but an ack already given stands.
- `OneCopyPerKey`: two producer replicas whose batches all reach one node (the stream's
  preferred node) store each key once.
- `DedupLosesNothing`: every key of an acknowledged batch is kept (not a tombstone) in some
  durable copy.
- `Converge` (liveness, under weak fairness of commit, flush, restart and repair): every node
  ends up with every batch some node holds durably.
- `EndCoversAcked`: the ends of any `EndNeed` nodes, merged per log, cover every acknowledged
  batch, while no node has lost a copy.

**Bounds.** Three nodes in three domains (four for `MC_end4*`), `copies = 2`, two producer
replicas sending the same key, one attempt per batch plus repair.

| Config | Faults | Checks | Distinct states |
|---|---|---|---|
| `MC_append` | 1 power loss, 1 lost disk | `AckedSurvive`, `AckedInCopiesDomains`, `NoConflictingAcks`, `EndCoversAcked` | 1,423,165 |
| `MC_dedup` | 2 power losses | `AckedSurvive`, `DedupLosesNothing` | 570,582 |
| `MC_one_index` | none, one entry node | `OneCopyPerKey`, `DedupLosesNothing` | 527,893 |
| `MC_conflict` | 1 power loss, 1 lost disk; two variants of one identity | `AckedSurvive`, `NoConflictingAcks` | 91,985 |
| `MC_converge` | write caching, 1 power loss | `Converge` | 6,967 |
| `MC_end4_fixed` | none, four nodes, 3 ends | `EndCoversAcked`, `AckedInCopiesDomains` | 880 |
| `MC_end4` (expect) | none, four nodes, 2 ends | `EndCoversAcked` fails | |
| `MC_dedup_caching` (expect) | write caching, 1 power loss | `DedupLosesNothing` fails: open finding below | |

**Found and fixed.** `Client::end` merged the ends of `min(n, 2)` nodes. An acked batch is on
`copies` nodes, so only `n - copies + 1` ends are sure to include one: on four nodes with 2
copies two ends can both miss an acked batch, and a reader starting at the end reads acked
events as new (`MC_end4`). Now `n - copies + 1`. Test:
`the_end_of_a_stream_on_four_nodes_merges_enough_ends_to_meet_every_acked_copy` (failed first:
`end` was empty).

**Found, open: dedup can lose an acknowledged event.** Batch `a` keeps event key `k`; batch `b`
(the other producer replica) arrives at the same node, its `k` is dropped as a duplicate, and
`b` is acknowledged once it is durable on 2 domains. Nothing waits for `a` to be: if `a` is
still only on the accepting node and that node loses `a` (a power loss before the deferred
fsync under write caching, live-ingestion's setting; or a lost disk), `k` is gone although `b`
was acknowledged (`MC_dedup_caching`: 8 steps; with a lost disk, `MC_append` with
`DedupLosesNothing` added). Without write caching and without a lost disk it holds
(`MC_dedup`). If `a`'s producer keeps retrying `a`, the retry restores `k`; if it crashed too,
`k` is lost. Fixing it is a design choice, left to the owner:
1. hold `b`'s ack until every batch that claimed one of its dropped keys is acknowledged (or
   durable on `copies` domains): no extra copies, a little ack latency for the second replica,
   and the accepting node has to drive a claimer it could not replicate (today only repair
   does);
2. drop a duplicate only once its claimer is acknowledged: no latency, but replicas sending
   within an ack round trip of each other (the common case) keep both copies;
3. accept it and document it next to the failover limit (§7.4 of `docs/PLAN.md`).

Also visible, not bugs: after a lost disk `Client::end` can be short until repair (a reader at
the end re-reads a little), and repair does not record dedup keys (a node that got a batch only
by repair keeps a later duplicate of it).

### Wire codecs: property tests in `crates/mesh`

`proto::props` (2,000 cases each, in the `test` job): batches of every format (PCB1, PCB2,
PCZ2) parse back to what was encoded, events, sequence numbers and times included; a valid
batch with up to three bytes changed or cut short, its CRC recomputed so it reaches the
structural checks, never panics `parse`, and a batch `parse` accepts is walked by every accessor
(`keys`, `try_events`, `keyed`, `without`, each `expect`ing what `parse` validated) without a
panic; arbitrary bytes under a matching CRC behind each magic never panic; acks, append and
replicate bodies and log keys round-trip and any bytes decode or are refused.
`node::props`: compacted-stream snapshots round-trip and damaged ones (CRC recomputed) are
refused. No bug found.

### The store against a reference model: `store::model`

A state-machine property test of `Store` (64 runs of up to 30 operations, in the `test` job):
reserves of random ranges (exact retries, partial overlaps, conflicting contents), durable marks
in any order and failed writes, checked after every step against a reference of the per-node
rules `Replication.tla` assumes (an exact retry is a duplicate, any other overlap a conflict, a
failed write frees its range, only durable batches are served) and of frontier arithmetic (the
durable prefix from 0, and one past the highest durable sequence). No discrepancy found.

## Consumers and positions: `spec/consumers/Positions.tla`

**Models.** A reader committing positions 1, 2, ... through any node (`commit_position`: a
keyed batch in that node's `__positions` writer log), replication and repair bringing each
commit to every node in any order, each node serving per key the newest value it holds
(`apply_compacted`); and a consumer reading one writer log by cursor, failing over between nodes
that hold different parts of it (`Store::read_cursor` stops at the first batch the node lacks;
`Consumer::take` advances past each batch).

**Abstracts away.** Payloads, several groups and streams (keys are independent), several
writer logs in one stream (`read_cursor` handles each log on its own), retention, max_bytes
(a poll returns any number of batches).

**Properties.** `ServesNewestHeld` (a node serves the newest commit it holds), `NoRegression`
(a node's served position never goes back; temporal), `NoLossNoRepeat` (a consumer failing over
reads each sequence once, in order, none skipped).

| Config | Bounds | Distinct states |
|---|---|---|
| `MC_positions` | 3 nodes, 3 commits | 33,900 |
| `MC_cursor` | 3 nodes, sequences 0..4 | 144,495 |
| `Nightly_positions` | 3 nodes, 4 commits and sequences 0..1 together | 15,173,327 |
| `MC_positions_lsn` (expect) | newest by LSN: `ServesNewestHeld` fails | |

**Found and fixed.** A compacted stream kept, per key, the value logged last on the node. Each
node logs commits in its own order, so a node that got commit 2 by replication and commit 1 by
repair served commit 1: a reader failing over there re-read what it had committed past
(`MC_positions_lsn`, 3 steps), and producers' compacted state could differ by node. The newest
value is now the last in read order, (event time, writer, epoch, seq), the order every node
with the same content agrees on; a position commit carries the commit time, increasing on each
node. Snapshots are format 2 (`PCS2`, with each key's read order); format 1 still reads, its
values older than any event. Test: `a_node_that_gets_two_commits_newest_first_serves_the_newest`
(failed first: it served `(a, 5)` while holding `(a, 9)`).
Known ceiling: two commits through two nodes closer together than the nodes' clock skew may
keep the earlier one (an order from the reader would remove it).

## Not verified, and why

- **Loom for the storage write path.** One OS thread owns the `Log`; other threads only read
  through `Reader` (atomics with acquire/release, `RwLock`s, a `Mutex` around the tail cache).
  There is no lock-free protocol for Loom to explore, and the publication order (`next_lsn`
  stored after the entries) is a single release store, covered by
  `readers_see_appends_concurrently_and_hit_the_tail_cache`.
- **The mesh codecs under Kani.** They are built on `bytes::Bytes` and anyhow errors; even a
  10-byte `Ack::decode` did not finish in 300 s (atomics, vtables and allocation behind them).
  Property tests above cover them instead, at much larger sizes.
- **P or Stateright for the protocol.** TLC finds the counterexamples above in seconds on
  models a page long; a second model closer to the Rust code would mean keeping two models in
  step with no new class of bug in reach. Apalache: the state spaces are small enough for
  explicit checking, so symbolic checking buys nothing yet.
- **Index and segment header round trips under Kani.** They need a `File`; their decoders are
  proved panic-free above and the round trips are unit-tested.

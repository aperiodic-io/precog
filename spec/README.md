# Formal verification

What is checked, with which tool, to what bound, and what is left out. Every check runs in CI
on each pull request (TLC in `tla`, Kani in `kani`, the property and model tests in `test`), with
bigger bounds in `nightly.yml`; `formal-coverage` fails a pull request whose changed code no
property or model test runs (below).

| Part | Tool | Why this tool | Where |
|---|---|---|---|
| Crash recovery of one segment, group commit, write caching | TLA+ / TLC | Exhaustive interleavings of writes, writeback in any order, power loss and media damage; small state spaces, no infrastructure | `spec/storage/SegmentLog.tla` |
| Segment roll, directory fsyncs, the two-slot manifest | TLA+ / TLC | A power loss between any two steps of a multi-file protocol | `spec/storage/SegmentRoll.tla` |
| Decoders of records, manifest slots and index files | Kani | Bounded proofs over every byte pattern, at the code itself | `#[cfg(kani)] mod proofs` in `crates/storage/src/{record,log}.rs` |
| Leaderless appends at k of n, failure domains, retries, conflicts, repair, dedup, write caching, `Client::end` | TLA+ / TLC | The protocol's failure modes are interleavings of messages, power losses and disk losses across nodes | `spec/mesh/Replication.tla` |
| Reader-group positions and compacted streams across nodes; cursor reads with failover | TLA+ / TLC | Commits and batches reach nodes in any order | `spec/consumers/Positions.tla` |
| Live subscriptions: the start point, per-log order, held batches | TLA+ / TLC | Batches become durable out of order around the moment a feed starts | `spec/livefeed/LiveFeed.tla` |
| Wire codecs: batches (every format), acks, append and replicate bodies, log keys, compacted-stream snapshots | proptest | The bytes come from the network or the disk; Kani does not finish them (below) | `mod props` in `crates/mesh/src/{proto,node}.rs` |
| RPC frame reader | proptest | Any byte stream, cut into any reads | `rpc::props` |
| The segment log's read APIs through appends, rolls, retention, lost index files and reopens | proptest, model-based | The code against a list of records, after every step | `model` in `crates/storage/tests/log.rs` |
| The store's identity and frontier rules | proptest, model-based | | `store::model` |
| Dormant streams: eviction, wakes, checkpoints racing changes, crashes and the restart from a checkpoint | TLA+ / TLC | Interleavings of a checkpoint taken under the lock and written without it, evictions, changes and power losses | `spec/store/Dormant.tla` |
| Dormant streams at the code: every query of a store that puts its streams out of RAM, fails checkpoints and reads, and crashes, against one that keeps them | proptest, model-based | A twin store is the reference: what RAM held is what must come back | `store::model_dormant` |
| Summaries and the checkpoint on disk | proptest | Round trips, every damage refused, any bytes, a number in one LEB128 form only, a commit read back after a reopen | `summary::props` |
| Repair following a peer's log (`Control::Inventory`) through peer crashes that lose an unsynced tail and reuse its LSNs, follower crashes and lost disks | TLA+ / TLC, proptest | A cursor into another node's log is only as good as that log's history | `spec/mesh/Inventory.tla`, `store::props_rejoin` |
| The WAL: group commit, ack order, a failed fsync | proptest, model-based | Grouping and the fsync rule at the code | `wal::model` |
| The WAL's write-caching gate: callers write under the log's lock, a roll or retention diverts them to the WAL thread | TLA+ / TLC | Its bugs are interleavings of worker threads and the WAL thread over two locks and a queue | `spec/wal/WalGate.tla` |
| The segment cleaner: each stream's own retention under the node cap, through slow batches and restarts | proptest | After every pass: the log within the cap, every log the newest suffix of what was written to it (no hole, once, byte for byte), a stream within its own retention (and within one batch of it unless the cap cut it) | `node::props_cleaner` |
| The dedup window and its cap; an idle stream's index swept | proptest, model-based | Against a map of each id's last-kept second | `dedup::model` |
| Restarts: a compacted stream's newest values, an older node's data directory rebuilt with its floors | proptest | | `node::props_restart` |
| The whole path (SDK, mesh client, RPC, node, store, WAL) on a 3-node cluster, one node down then repairing | proptest, model-based | Reads, resumes, ends and latest values against what was acked | `crates/client/tests/model.rs` |

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

## Formal coverage: what the checks reach, and the gate

A property or model test is a test in a module named `props` or `model` (or `props_*`,
`model_*`). The `formal-coverage` job runs only those under `cargo llvm-cov` and fails if a line
the pull request adds or changes in `crates/{storage,mesh,client}/src` is not run by one
(`scripts/formal-gate.py`; test code, lines llvm-cov does not instrument, the `precog-node` CLI
and TCP dialing are not gated). Kani and TLC report no lines; the tests beside them do.

```sh
cargo llvm-cov nextest --workspace --exclude storage-bench --exclude precog-bench \
  -E 'test(/(^|::)(props|model)(_[a-z_]+)?::/)' --lcov --output-path formal.lcov
scripts/formal-gate.py formal.lcov origin/main
```

Lines of each module (test code left out) the property and model tests run, before this audit
and after it:

| Module | Before | After | Checks | Not reached, and why |
|---|---|---|---|---|
| `storage/src/record.rs` | 97% | 97% | Kani, `SegmentLog.tla`, crash props | error formatting |
| `storage/src/log.rs` | 58% | 88% | `SegmentLog.tla`, `SegmentRoll.tla`, Kani, crash props, `model` | roll failpoints and damaged-file paths (unit-tested), spare pacing |
| `mesh/src/proto.rs` | 90% | 98% | `proto::props` | |
| `mesh/src/wal.rs` | 0% | 95% | `wal::model` | the deferred flush's error log |
| `mesh/src/dedup.rs` | 0% | 82% | `dedup::model`, `Replication.tla` | `Streams` (per-stream maps, unit-tested) |
| `mesh/src/rpc.rs` | 0% | 76% | `rpc::props`, cluster model | timeouts and closed connections |
| `mesh/src/store.rs` | 27% | 46% | `store::model`, cluster model, `Replication.tla`, `Positions.tla`, `LiveFeed.tla` | retention, time seeks, live feeds (unit and integration tests) |
| `mesh/src/node.rs` | 5% | 53% | cluster model, `Replication.tla`, `Positions.tla`, `LiveFeed.tla` | subscriptions, metrics, retention, conflicts, restart |
| `mesh/src/client.rs` | 0% | 35% | cluster model | retries on unknown acks, subscriptions |
| `client/src/lib.rs` | 0% | 85% | cluster model, `Replication.tla` (`Client::end`) | time starts, compression off |
| `mesh/src/bin/`, `transport.rs` | 0% | | acceptance suite | process setup, DNS (not gated) |

The gate applies to new code; the "not reached" column is what the next model extensions
should take (node restarts in the cluster model need a `Node` that can stop).

## Storage: `spec/storage`

### `SegmentLog.tla`: one active segment

**Models.** `Log::append` (one write per group; without write caching an fsync per group before
the ack, one group at a time; with write caching an ack once written, the deferred flush outside
the log's lock while appends go on (`Log::begin_flush`, `Flush::sync`, `Log::end_flush`), and an
inline sync once unsynced plus in-flight bytes reach twice `flush_bytes`), the kernel writing dirty pages back in any order, a
power loss that keeps each unsynced record whole, torn or not at all, media damage to a
persisted record, an fsync that fails (Linux may drop the pages it could not write and mark
them clean, so a later fsync succeeds without them; the log then refuses everything until a
restart, `Error::Fatal`), and the recovery scan of `scan()` step for step: the forward scan with its LSN
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
| `MC_sync` | 5 records | ≤ 2 | 2 | media faults, 1 failed fsync | 425,867 |
| `MC_caching` | 6 | ≤ 2 | 1 | flush at 2, bound 6 (`2 × flush + group`), 1 failed fsync | 1,199,742 |
| `Nightly_sync` | 6 | ≤ 3 | 2 | media faults, 1 failed fsync | 3,959,473 |
| `Nightly_caching` | 6 | ≤ 2 | 2 | flush at 2, bound 6, media faults (`Prefix`) | 33,850,249 |
| `MC_caching_onebound` (expect) | 6 | ≤ 2 | 1 | bound 4 (`flush + group`, before #16): `NoFalseAlarm` fails | |
| `MC_sync_retry` (expect) | 4 | ≤ 2 | 1 | a failed fsync retried instead of fatal: `AckedSurvive` fails | |

`MC_sync_retry` is the rule in AGENTS.md (never retry an fsync and then ack) as a model: the
retry succeeds without the pages the failure dropped and acknowledges them.

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

## Dormant streams: `spec/store/Dormant.tla`

**Models.** A stream leaving RAM and coming back from its summary (`Inner::evict`,
`Inner::wake`); a checkpoint collected under the store's lock (`Store::begin_checkpoint`:
the changed streams' summaries, `cold_end`, `contiguous`, the LSNs in RAM, the forgotten ones)
and written later without it (`end_checkpoint`), or failing; records appended and marked
durable in any order, forgotten by a stream's own retention, absorbed in LSN order; a power
loss (RAM and an unwritten checkpoint gone, every record in the log recovered) and the restart
(`Store::open`): each summary, the RAM records of the checkpoint put back, the records past it
replayed as new unless forgotten.

**Abstracts away.** Batches' contents and sequence ranges (a record is its LSN and belongs to a
stream), frontiers (a stream's state is the set of records it serves), segment boundaries and
dropping (whole-segment drops follow a checkpoint in `Node::apply_retention`; the twin model and
`tests/dormant.rs` check them), redb (a commit is atomic and durable).

**Invariant.** `Exact`: a stream in RAM serves exactly its durable records its retention did not
forget, whether it never left RAM, left it and came back, or the node restarted (what retention
forgot after the last checkpoint is back after a crash; the next pass forgets it again).

**Bounds.**

| Config | Streams | Records | Crashes | Distinct states |
|---|---|---|---|---|
| `MC_dormant` | 2 | 3 | 2 | 453,888 |
| `Nightly_dormant` | 2 | 4 | 2 | 9,947,372 |
| `MC_dormant_evict_in_flight` (expect) | 1 | 2 | 0 | eviction while a checkpoint is in flight: `Exact` fails |
| `MC_dormant_clear_at_commit` (expect) | 2 | 3 | 0 | a stream clean once written, not once collected: `Exact` fails |
| `MC_dormant_replay_forgotten` (expect) | 1 | 3 | 1 | the restart replays a record forgotten past `contiguous`: `Exact` fails |

**Found.** An eviction between a checkpoint's collection and its write: the stream counts as
clean, leaves RAM, and wakes from its older summary (`MC_dormant_evict_in_flight`). Fixed: no
stream leaves RAM while a checkpoint is in flight, and a write that fails marks its streams
changed again. Test: `no_stream_leaves_ram_while_a_checkpoint_of_it_is_in_flight` (failed first).

### Dormant streams at the code: `store::model_dormant`

Two stores over two logs written alike (96 runs of up to 40 operations, and the cases below
every run): reservations, writes, durable marks in any order, failures, absorption, retention as
the node runs it (checkpoint, drop of whole sealed segments, trim), floors inside a segment,
streams' own retention; the second store also puts every stream it can out of RAM, writes
checkpoints that fail before or after they are durable, evicts with a checkpoint in flight,
fails its summary reads, and crashes back to its checkpoint. After every step both answer every
query alike: totals, frontiers, live starts, inventories, `has`/`get`/`durable_below`/
`log_batches` for every sequence, cursor reads, seeks by time, tails, event-time reads.

**Found** (each a fixed case of `the_cases_the_model_found_and_its_rare_paths`, failed first):
- `frontiers(None)` and `live_start(None)` read a dormant stream's summary as written, before the
  segments retention dropped since: a stale contiguous end (now such a stream is woken first).
- A restart put a batch back in RAM after a dropped segment had trimmed its log up to it, but
  did not move the log's contiguous end over it.
- The restart replayed a batch forgotten past `contiguous` as new (now the checkpoint lists
  them with the dead ones), and a batch replayed as new did not see the batches put back in RAM
  later in the log (now those are put back first).
- A floor inside a segment (store-level `trim`; the node drops whole segments) left dormant
  streams' totals counted until they woke: such a trim wakes them first.
- A stream's `ooo` flag missed a batch marked durable before its predecessor at a later LSN, so a
  seek by time (`cursor_at`) could stop before the lowest sequence (a bug on main too).

Not a dormancy bug, left as it is: a stream's retention floor compares (append time, sequence,
LSN) across its logs, so a batch logged in the same millisecond as a forgotten one with a lower
sequence, or written before it and marked durable after, is hidden once absorbed (on main too);
the twin model gives batches distinct append times and absorbs at a restart.

## Repair by following a peer's log: `spec/mesh/Inventory.tla`

**Models.** `Node::repair_peer` asking a peer for what it logged from a cursor
(`Control::Inventory`), the peer answering from `rejoin` (`Store::rejoin`), the follower's
checkpoint keeping its cursor; the peer losing its unsynced tail in a power loss (write caching)
and logging other records at those LSNs; the follower crashing back to its saved cursor or
losing its disk. One page is one record.

**Invariants.** `Covered`: a follower caught up with the peer's boot and log holds every
record the peer holds. `InLog`: its cursor never points past the peer's log.

**Bounds.** `MC_inventory`: 4 records, 3 crashes, 59,861 distinct states. `MC_inventory_norejoin`
(expect `Covered`): the peer answers from the cursor whatever boot it is in.
`MC_inventory_lastboot` (expect `Covered`): the peer goes back to its own boot's open only.

**Found.** Two crashes of the peer between a follower's requests: the boot in between logged
records below the second boot's open, which "back to where this boot opened" skips
(`MC_inventory_lastboot`). Fixed: a node keeps its boots (incarnation, where its log ended at
the open) in its checkpoint, written at the open, and answers a follower of an older boot from
the lowest open since (from the start for a boot it no longer knows). Test:
`a_peer_is_followed_again_from_the_first_open_after_the_boot_it_followed`, `store::props_rejoin`.

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
- `IndexCoversHeld`: a running node's dedup index holds the keys of every batch it holds,
  however the copy arrived (accepted, replicated, repaired, recovered).
- `Converge` (liveness, under weak fairness of commit, flush, restart and repair): every node
  ends up with every batch some node holds durably.
- `EndCoversAcked`: the ends of any `EndNeed` nodes, merged per log, cover every acknowledged
  batch, while no node has lost a copy.

**Bounds.** Three nodes in three domains (four for `MC_end4*`), `copies = 2`, two producer
replicas sending the same key, one attempt per batch plus repair.

| Config | Faults | Checks | Distinct states |
|---|---|---|---|
| `MC_append` | 1 power loss, 1 lost disk | `AckedSurvive`, `AckedInCopiesDomains`, `NoConflictingAcks`, `EndCoversAcked`, `IndexCoversHeld` | 1,171,030 |
| `MC_dedup` | 2 power losses | `AckedSurvive`, `DedupLosesNothing`, `IndexCoversHeld` | 455,955 |
| `MC_one_index` | none, one entry node | `OneCopyPerKey`, `DedupLosesNothing` | 487,933 |
| `MC_conflict` | 1 power loss, 1 lost disk; two variants of one identity | `AckedSurvive`, `NoConflictingAcks` | 91,985 |
| `MC_converge` | write caching, 1 power loss | `Converge` | 5,295 |
| `MC_end4_fixed` | none, four nodes, 3 ends | `EndCoversAcked`, `AckedInCopiesDomains` | 880 |
| `MC_end4` (expect) | none, four nodes, 2 ends | `EndCoversAcked` fails | |
| `MC_dedup_caching` (expect) | write caching, 1 power loss | `DedupLosesNothing` fails: open finding below | |
| `MC_repair_unfixed` (expect) | 1 power loss; repair not recording keys | `IndexCoversHeld` fails | |

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

**Found and fixed.** Repair stored a batch without recording its dedup keys (replication and
a restart's rebuild both record them), so a node that got a batch only by repair (it was down,
or lost its disk) accepted a later duplicate of its events: a producer replica's copy, or a
resend, landing on it once it was back as the stream's preferred node. Found by the cluster
model (`crates/client/tests/model.rs`: node 1 down, `e13` sent, node 1 joins and repairs, `e13`
sent again, read twice); `MC_repair_unfixed` keeps it. Test:
`a_node_that_got_a_batch_by_repair_drops_a_later_duplicate_of_its_events` (failed first: both
copies stored).

Also visible, not a bug: after a lost disk `Client::end` can be short until repair (a reader at
the end re-reads a little).

### Wire codecs: property tests in `crates/mesh`

`proto::props` (2,000 cases each, in the `test` job): batches of every format (PCB1, PCB2,
PCZ2) parse back to what was encoded, events, sequence numbers and times included; a valid
batch with up to three bytes changed or cut short, its CRC recomputed so it reaches the
structural checks, never panics `parse`, and a batch `parse` accepts is walked by every accessor
(`keys`, `try_events`, `keyed`, `without`, each `expect`ing what `parse` validated) without a
panic; arbitrary bytes under a matching CRC behind each magic never panic; acks, append and
replicate bodies and log keys round-trip and any bytes decode or are refused.
`node::props`: the streams' retention floors (PCF1) round-trip and damaged ones are refused.
`node::props`: compacted-stream snapshots round-trip and damaged ones (CRC recomputed) are
refused. No bug found.

### The store against a reference model: `store::model`

A state-machine property test of `Store` (64 runs of up to 30 operations, in the `test` job):
reserves of random ranges (exact retries, partial overlaps, conflicting contents), durable marks
in any order and failed writes, checked after every step against a reference of the per-node
rules `Replication.tla` assumes (an exact retry is a duplicate, any other overlap a conflict, a
failed write frees its range, only durable batches are served) and of frontier arithmetic (the
durable prefix from 0, and one past the highest durable sequence). No discrepancy found.

### The segment log against a list of records: `model` in `crates/storage/tests/log.rs`

Appends of 1 to 3 records with keys from a set of three (16 KiB segments, so rolls and sealed
index files come quickly), flushes, retention by size and by age, dropping the tail cache,
deleting a sealed segment's index file and reopening, and clean reopens, with and without write
caching; after every step `read`, `entries`, `sealed` and `by_key` agree with the model:
records below the floor are gone, the sealed segments tile the log from it, each one's by-key
lookup holds exactly its records of the key by `first_seq` (the order of `IDX_VERSION` 2),
retention by age drops exactly the leading run of segments whose records are all older, and a
lost index file is rebuilt. It catches a by-key section sorted by record index instead of
`first_seq`. No discrepancy found.

### The WAL: `wal::model`

Bursts of plain and replacing appends submitted before any is awaited (so the writer groups
them), retention, and the data fsync failing after a random number of syncs, with and without
write caching: acks arrive in submit order with consecutive LSNs, each acked batch reads back
with its bytes and its key (replacing or not), a failure fences the WAL (`fatal`), and from then
on nothing is acknowledged or retained. No discrepancy found.

### The write-caching gate: `spec/wal/WalGate.tla`

**Models.** Workers submitting their own batches one after another under write caching: the
gate's mutex, then the log's; a batch that fits is written by the worker, one that rolls raises
the gate and is queued; with the gate up a batch is queued under the gate. The WAL thread: at the
top of its loop, with the gate up, the next queued batch, or lifting the gate when nothing is
queued; a blocking receive; retention jobs, which raise the gate before taking the log; queued
batches appended by the thread (which may roll). Slow steps (a roll, a retention job) hold the
log's lock.

**Abstracts away.** Payloads, the deferred fsync (short, under the lock only to begin and end
it), the sync past twice `flush_bytes` on the worker (bounded by `flush_bytes`), errors.

**Checks.** `InOrder` (each worker's batches reach the log in its submit order),
`NeverWaitsOnSlow` (no worker waits for the log's lock while a roll or a retention job holds it),
`Complete` (once nothing can happen, every batch is logged and every job ran).

| Config | Bound | Result |
|---|---|---|
| `MC_gate` | 2 workers x 2 batches, 1 job, any batch may roll | all hold, 1,384 states |
| `MC_gate_ungated` (expect) | the code before the gate: a worker rolls on itself | `NeverWaitsOnSlow` fails |
| `MC_gate_clear_early` (expect) | the gate lifted with batches still queued | `InOrder` fails |

**Found.** Before the gate (#16 to #24) a worker rolled the segment on itself under the log's
lock, and every worker that submitted meanwhile waited for the roll's syncs (and a zero-fill
with no spare), as it did for retention on the WAL thread (`MC_gate_ungated`). Test:
`wal::tests::under_write_caching_a_submit_never_waits_for_a_roll` (failed first: the submit
waited 129 ms of a 129 ms roll).

### RPC frames: `rpc::props`

Frames of every kind with random ids and bodies, the stream cut into random reads: they read
back in order and end cleanly; cut inside a frame, the frames before the cut read back and the
read fails; any bytes read as frames or fail, never a panic. No bug found.

### The dedup window: `dedup::model`

Checks, records (what a replica or repair keeps) and clock ticks against a map of each id's
last-kept second. Without the cap dedup is exact (a duplicate iff kept within the window,
inclusive); at any cap it never claims a duplicate for an id not kept within the window, and
catches every id kept within `effective_window_s`.

**Found and fixed.** Past `max_entries`, `record` dropped the oldest quarter of the recorded ids
without shortening the window, so `effective_window_s` (`precog_dedup_window_seconds`) kept
reporting ids as remembered that were gone, and could drop ids of the current second (the
check path keeps a second's ids: the cap is soft within a second). It now raises the window's
floor past what it drops, and never drops the current second. Test:
`at_the_cap_dedup_is_sound_and_keeps_its_effective_window` (failed first: an id recorded at 100
missed at 100 within a reported 0 s window).

### The whole path: `crates/client/tests/model.rs`

A 3-node cluster on loopback, optionally one node down from the start until it joins and runs
a repair pass. Two producer replicas send events with dedup ids from a set of 24, a compacted
stream takes puts; then a reader from the beginning reads each acked id exactly once and each
writer in order, a reader that commits partway and resumes from the commit reads the rest
exactly, a reader at the end reads nothing, and `latest` is the newest value of every key. 256
runs of up to 24 steps.

**Found and fixed.** `Consumer::poll` (Rust and Go) returned no events whenever the batches it
fetched held none for the reader (a producer replica's copy whose events dedup all dropped, or
one the cursor was past), though it is documented as empty only when caught up: a reader that
reads until an empty poll stopped short (here: everything after b's copy of an event a sent).
It now reads on until events come back or no batch does. Tests:
`a_poll_is_empty_only_when_caught_up_even_past_a_batch_dedup_emptied` (Rust,
`crates/client/tests/sdk.rs`) and `TestAPollIsEmptyOnlyWhenCaughtUpEvenPastABatchDedupEmptied`
(Go); both failed first, reading only `t0`. And repair's dedup keys, above.

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

## Live subscriptions: `spec/livefeed/LiveFeed.tla`

**Models.** A live (not from-the-start) subscription to one node, for one writer log whose
batches become durable there in any order: `Store::live_start` (the start position, and the end
of the log's run below it), the empty push that confirms the start before `subscribe` returns,
`push_loop` reading the durable order from the start, `in_log_order` holding a batch until its
predecessor came, and the client's `Subscription::accept` joining the log at the first batch it
sees and holding batches out of order.

**Abstracts away.** Several logs and streams (each log is handled on its own), retention during
a feed, `max_bytes` paging of `tail`, the wake-up (`Notify`) between appends and the feed (the
feed re-reads from its position on every wake-up, so a late wake-up only delays it).

**Properties.** `InOrder` (the client delivers consecutive sequences) and `NothingStuck` (once
every batch arrived and the feed caught up, nothing is held on either side and every batch made
durable after `subscribe` returned was delivered: the start point is confirmed).

| Config | Bounds | Distinct states |
|---|---|---|
| `MC_livefeed` | 5 batches, any durable order, subscribe at any point | 10,461 |
| `Nightly_livefeed` | 7 batches | 906,177 |
| `MC_livefeed_skip` (expect) | the feed before the fix: `NothingStuck` fails | |

**Found and fixed.** The feed skipped batches made durable before it started, including ones
behind a gap in their log. When the gap filled, it pushed the gap's batch, skipped the next
(durable before the start), and pushed the one after; the client, which takes a log in
sequence from the first batch it sees, held that one and every later batch of the log for good.
A node makes a log durable out of order whenever a producer has several batches in flight
(the SDK keeps 8) or repair fills a hole. Now a batch durable before the start that follows the
gap is pushed in sequence. Test: `a_live_feed_that_joins_behind_a_gap_delivers_everything_after_it`
(failed first: the feed stalled after batch 2).

## Not verified, and why

- **Loom for the storage write path.** Writers take the `Log` under one mutex (the WAL thread,
  and under write caching the callers, whose ordering against it is `WalGate.tla`); other
  threads only read through `Reader` (atomics with acquire/release, `RwLock`s, a `Mutex` around the tail cache).
  There is no lock-free protocol for Loom to explore, and the publication order (`next_lsn`
  stored after the entries) is a single release store, covered by
  `readers_see_appends_concurrently_and_hit_the_tail_cache`.
- **Loom for the live feed.** The feed waits on a `tokio::sync::Notify` (registered before it
  checks the store, the documented no-lost-wakeup pattern) and re-reads the store from its
  position on every wake-up; Loom does not model tokio's primitives, and the model above covers
  what the feed computes from the store, at every interleaving of appends with it.
- **The segment cleaner in TLA+** (#28: copy forward, drop, a replacement in between, a crash
  anywhere). Its steps run on one task and the WAL thread; the only concurrent writer of the
  batches it moves is a replacement, and the two are serialised by `Node::clean`, with each
  copy group re-checking what is still retained under it. What it must keep is checked at the
  code instead: `node::props_cleaner` (writes, slow batches, replacements, passes, restarts,
  damaged segments), the failpoint test at every step between copy and drop
  (`a_crash_anywhere_between_copy_and_delete_keeps_every_retained_batch_once`) and
  `props_majority` in `crates/mesh/tests/cluster.rs`. A model of copy, replacement and crash
  interleavings is the next extension of `SegmentLog.tla`.
- **The mesh codecs under Kani.** They are built on `bytes::Bytes` and anyhow errors; even a
  10-byte `Ack::decode` did not finish in 300 s (atomics, vtables and allocation behind them).
  Property tests above cover them instead, at much larger sizes.
- **P or Stateright for the protocol.** TLC finds the counterexamples above in seconds on
  models a page long; a second model closer to the Rust code would mean keeping two models in
  step with no new class of bug in reach. Apalache: the state spaces are small enough for
  explicit checking, so symbolic checking buys nothing yet.
- **The summaries' decoders under Kani** (`summary::decode_stream`, `decode_checkpoint`): mesh
  code, like the codecs above; `summary::props` covers them (round trips, every one-byte flip,
  cut and extension refused, any bytes behind a valid checksum, a number longer than it needs or
  past 64 bits refused). redb itself (its pages, its
  commit) is trusted, as the filesystem is: `a_commit_is_read_back_after_a_reopen` and the crash
  tests (`summary:before_commit`, `summary:after_commit`) exercise it.
- **Dormant streams across nodes in TLA+.** Dormancy is local to a node: a peer's repair wakes
  what it compares (`Inventory.tla` covers what it follows), and `Replication.tla` abstracts a
  node's store as the set of what it holds, which eviction does not change.
- **Index and segment header round trips under Kani.** They need a `File`; their decoders are
  proved panic-free above and the round trips are unit-tested.

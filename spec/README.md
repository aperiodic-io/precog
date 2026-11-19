# Formal verification

What is checked, with which tool, to what bound, and what is left out. Every check runs in CI:
the `formal` job on each pull request, bigger bounds in `nightly.yml`.

| Part | Tool | Why this tool | Where |
|---|---|---|---|
| Crash recovery of one segment, group commit, write caching | TLA+ / TLC | Exhaustive interleavings of writes, writeback in any order, power loss and media damage; small state spaces, no infrastructure | `spec/storage/SegmentLog.tla` |
| Segment roll, directory fsyncs, the two-slot manifest | TLA+ / TLC | A power loss between any two steps of a multi-file protocol | `spec/storage/SegmentRoll.tla` |
| Decoders of records, manifest slots and index files | Kani | Bounded proofs over every byte pattern, at the code itself | `#[cfg(kani)] mod proofs` in `crates/storage/src/{record,log}.rs` |

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
the ack, one group at a time; with write caching an ack once written and an fsync once
`flush_bytes` are unsynced), `Log::flush`, the kernel writing dirty pages back in any order, a
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
| `MC_caching` | 6 | ≤ 2 | 2 | flush at 2, bound 4 | 224,200 |
| `Nightly_sync` | 7 | ≤ 3 | 3 | media faults | see nightly |
| `Nightly_caching` | 7 | ≤ 2 | 2 | flush at 3, bound 5, media faults (`Prefix`) | 13,972,478 |

**Found.** Under write caching several groups are unsynced at once and the kernel writes them
back in any order, so a power loss can keep a later group and lose an earlier one. The scan
read that as damage followed by a later group and refused to open: a node of live-ingestion's
configuration would not restart after a power loss (`MC_caching_unfixed`, 5 steps). Fixed: at
most `flush_bytes` plus one group can be unsynced, so with write caching a hole followed only by
records within that bound is the unsynced tail and is dropped as a whole (repair refills it from
the replicas); a hole followed by more than that is still damage and fails closed. Tests:
`a_power_loss_under_write_caching_drops_the_unsynced_tail_whatever_reached_disk` (failed first)
and `under_write_caching_damage_before_more_than_the_unsynced_bound_still_fails_closed`.

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

## Not verified, and why

- **Loom for the storage write path.** One OS thread owns the `Log`; other threads only read
  through `Reader` (atomics with acquire/release, `RwLock`s, a `Mutex` around the tail cache).
  There is no lock-free protocol for Loom to explore, and the publication order (`next_lsn`
  stored after the entries) is a single release store, covered by
  `readers_see_appends_concurrently_and_hit_the_tail_cache`.
- **Index and segment header round trips under Kani.** They need a `File`; their decoders are
  proved panic-free above and the round trips are unit-tested.

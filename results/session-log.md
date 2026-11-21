# Precog under a session-log workload (2026-11-21)

What main does with the workload [`docs/SESSION-LOG.md`](../docs/SESSION-LOG.md) plans for: one
stream per agent session, events of kilobytes to megabytes. Measured on main at `0e5df29`.

## Setup

One 4-vCPU VM, 15 GiB. Three `precog-node serve` processes on loopback from
`precog-node init --nodes 3 --segment-mb 256`: the defaults, so one runtime thread per node and
an fsync before every ack. Every append is acknowledged on 2 of 3 nodes. The client is the mesh
client (`MeshClient::append`), one event per batch unless noted.

The probe that produced these numbers is not in the repository yet (plan item P1 puts it in
`bench/session`).

## Event sizes

One writer, one append in flight, incompressible payloads, 30 appends per size.

| Event | ack p50 | ack p99 | MiB/s |
|---:|---:|---:|---:|
| 1 KiB | 0.35 ms | 0.71 ms | 2 |
| 64 KiB | 0.80 ms | 1.03 ms | 74 |
| 1 MiB | 5.76 ms | 16.9 ms | 125 |
| 4 MiB | 19.3 ms | 28.7 ms | 162 |
| 8000 KiB | 41.4 ms | 68.8 ms | 149 |
| 8 MiB and above | refused: `batch too large` (`MAX_BATCH_BYTES`) | | |

## Neighbours

A writer of 10 × 256-byte events every 5 ms, one append in flight, for 10 s: alone, then beside
two writers appending large events back to back (about 150 MB/s between them, which is this
VM's disk).

| | ack p50 | ack p99 | ack max | appends done in 10 s |
|---|---:|---:|---:|---:|
| alone | 0.9 ms | 2.9 ms | 23 ms | 2001 |
| beside 2 writers of 1 MiB events | 6.0 ms | 48.7 ms | 890 ms | 879 |
| beside 2 writers of 4 MiB events | 16.5 ms | 202 ms | 1116 ms | 352 |

The small writer shares the node's one group commit with the large ones.

## Sessions

64 sessions × 50 LLM calls, 16 sessions in flight, one stream per session, one event per call,
each acknowledged before the session's next. Synthetic text (a skewed pick from 4,096 random
words). A call's request is a 20 KB system prompt and tool schemas plus every message so far, and
adds a message of 0.4 to 6.4 KB. Every event was read back and compared byte for byte.

"AWD1" is agentwire's delta codec applied by the client: each event is a delta against the event
before it in its writer log, and every 64th is stored whole. The broker is unchanged.

| What each call logs | Stored as | Logical | Stored per node | calls/s | ack p50 | ack p99 |
|---|---|---:|---:|---:|---:|---:|
| the whole request | as sent | 350 MB | 350 MB | 2,249 | 5.90 ms | 23.7 ms |
| | lz4 per batch (the SDK's default) | 350 MB | 260 MB | 1,251 | 5.86 ms | 32.5 ms |
| | AWD1 | 350 MB | 7.6 MB | 6,107 | 1.94 ms | 8.8 ms |
| only the new message | as sent | 11.2 MB | 11.4 MB | 9,162 | 1.15 ms | 7.4 ms |
| | lz4 per batch | 11.2 MB | 10.9 MB | 10,053 | 1.08 ms | 7.7 ms |
| | AWD1 | 11.2 MB | 6.5 MB | 7,775 | 1.48 ms | 7.8 ms |

A reader starting at the last-but-one call of an AWD1 session (48 events into its chain) went
back to the chain's first event and decoded forward in 9.3 ms (p50 of 32, max 10.7 ms).

## Caveats

- **One VM.** The three nodes share one disk and 4 vCPUs with the client, so "2 of 3" is not
  independent hardware and the large writers saturate the disk all three write to.
- **Synthetic text** compresses worse than real prompts (zstd gets about 1.7x on it): the
  whole-request ratios are a floor. agentwire measured 105x on public agent traces
  (its README, "Storing logs and traces").
- **The neighbour test is the extreme case**: two writers at the disk's throughput.
- **A few runs each.** The run-to-run spread on this VM is up to about 20%.

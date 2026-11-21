# Precog as a durable session log: plan

Status: adopted for the session-log product (2026-11-21). It does not change
[`PLAN.md`](PLAN.md), whose first customer, live-ingestion's raw cluster, still comes first and is
the only source of production hours. It takes [`S2-PLAN.md`](S2-PLAN.md)'s S0 decision one way:
Precog is the **durable log of agent sessions**, the storage and transport underneath
observability products, not one of them. Dashboards, search, evals and analytics are out of scope.

Evidence for the gaps below: [`results/session-log.md`](../results/session-log.md) (measured on
main, 2026-11-21) and the code as it is on main.

## 1. The product

One stream per agent session. Every LLM call, tool call and state change is an event, appended
durably and readable live or later, from any position, for as long as the stream's retention says.

| | |
|---|---|
| Unit | A session stream: created by its first append, named `<tenant>/<session>` (any prefix scheme) |
| Scale | Millions of streams per cluster, most idle; events from 100 bytes to several MiB |
| Writers | One or a few per session (the agent, its tools, a gateway); a stream-level order is opt-in (owned streams, phase 2) |
| Readers | A live tail (a UI, a second device), a resumed reader (a reload), a replay or export |
| Retention | By class, hours to months, and a delete on request |
| Tenants | Many, each confined to its own prefix |

### Contract (added to PLAN.md's four guarantees)

5. **A stream's retention is its class's.** A stream keeps what its class retains whatever other
   streams write, and never serves a batch older than its class's age limit.
6. **A tenant reaches only its own streams.** A token is confined to stream prefixes and to
   reading or writing; only a node's peers replicate.
7. **A deleted stream stays deleted.** Repair never brings it back, on any node, after any crash.

## 2. Where main is today

| Gap | Today | Evidence |
|---|---|---|
| Retention per stream | Static node config keyed by the exact stream name. A stream not named there loses its oldest data at the node cap, whoever owns it | `NodeConfig::streams`, `Node::apply_retention` |
| Tenancy | One shared token. A token holder may claim any peer id. No scopes, quotas or metering | `Node::authenticate` |
| Delete | None. A trim is local to a node and logical | `Store::trim_stream` |
| Transport security | Plain TCP | `transport.rs` |
| Isolation | One log and one group commit per node: a 256-byte writer's p99 ack went from 3 ms to 202 ms beside two writers of 4 MiB events | results, "Neighbours" |
| Event size | A batch is at most 8 MiB, refused above. Nothing above 256 bytes was benchmarked before | results, "Event sizes" |
| Stream-level order | Per writer log only. Owned streams are a POC (`crates/lab/src/s2`) | S2-PLAN §3.2 |
| API | Native TCP, Rust and Go SDKs. The REST and SSE gateway is a POC outside the workspace | `crates/s2-lab` |
| Re-sent context | Stored as sent: 350 MB for 64 sessions that hold 11 MB of new content | results, "Sessions" |
| Metrics | Labels per stream and per group: unbounded with a stream per session | `Node::metrics` |
| Operations | No multi-machine benchmark, soak, backup, or upgrade test | S2-PLAN caveats, PLAN M7 |
| Capacity | 3 static nodes, each holding everything; no cold tier | PLAN M4, M8 |

What is already there and stays as it is: durable acks on 2 of 3, idempotent retries, repair,
cursors valid on every node, reader groups, live feeds, dormant streams on disk (10M streams in
about 600 MiB of heap), the segment cleaner, and the verification gates.

## 3. Phases

Each row is one pull request in a stack (§5) unless it says otherwise. "Verified by" names the
formal check the PR adds, in the same PR (AGENTS.md).

### Phase 1: the session-log core in the broker

| ID | Change | Done when | Verified by |
|---|---|---|---|
| A1 | **Stream classes.** A `streams` key ending in `*` configures every stream with that prefix; the longest match wins, an exact name wins over any class. Retention passes trim the class's streams in RAM, a stream is trimmed before a read serves it, and the cleaner copies forward what class streams retain | Two tenants' session streams keep exactly their class's retention on one node, through the node cap, idle streams and restarts; a pass costs nothing per idle stream | Property tests: class lookup against a reference; a store with classes against one with every stream named; the cleaner's properties rerun with classes |
| A2 | **Scoped tokens.** Tokens in the node config, each with stream prefixes and `read`/`write`; a separate peer token; the shared token keeps working as an unscoped one | A tenant token cannot read, write, tail, commit or list another prefix, and cannot replicate; acceptance scenarios on 3 nodes | Property test: every request kind against a reference of what its token may reach; `Replication.tla`: a client never replicates |
| A3 | **Stream delete.** `Control::Delete` writes a tombstone the mesh replicates and repair follows; a node with the tombstone refuses older batches of the stream | A stream deleted while a node is down is deleted there after repair and never returns; a new stream of the same name works | TLA+: a `Delete` action in `Replication.tla` with `DeletedStaysDeleted`; model test of the store with deletes |
| A4 | **Events above a batch.** A client-side chunking of one event over several batches, reassembled by the reader, or a raised cap if measurement allows it | A 64 MiB event round-trips; a reader resuming inside it restarts at its first chunk | Property test of the chunk codec and the reader |
| A5 | **Fair shares of the log.** Per-token byte rate limits at the accepting node, and group commits that take a bounded share from one connection | The neighbour test's small writer stays within 3x its p99 alone | Model test of the limiter; the neighbour test as a perf-gate case |
| A6 | **Bounded metrics.** Per-class and per-token series in place of per-stream ones, with the per-stream ones opt-in by exact name | `/metrics` has the same number of series at 10 streams and at 1M | Property test of the series set |
| A7 | **TLS.** rustls on the client and peer transports, certificates from files, reload without restart | The acceptance suite passes with TLS on; a plain client is refused | Acceptance scenarios (the handshake is the library's) |

### Phase 2: a stream-level order (S2-PLAN C1)

| ID | Change | Done when | Verified by |
|---|---|---|---|
| B1 | **Owned streams in the node.** One owner task per stream assigns dense sequence numbers, checks `match_seq_num` and fencing tokens, records `fence` and `trim`; majority `copies` enforced | S2's semantics as acceptance scenarios | TLA+: owner takeover with a zombie owner; a Porcupine-style linearizability check under SIGKILL and SIGSTOP |
| B2 | **Owner failover.** A per-log epoch floor, takeover from a majority tail, truncation of minority losers before repair spreads them | A zombie stops at its next batch; no sequence number is lost or reused | The same model, with the floor |

### Phase 3: the gateway (S2-PLAN C3), a workspace crate

| ID | Change | Done when |
|---|---|---|
| C1 | **REST and SSE**, S2's shapes, over owned streams; token auth from A2 | S2's CLI and SDK examples run unchanged |
| C2 | **OTLP/HTTP ingest** mapped onto session streams by a resource attribute | The OpenTelemetry SDK's exporter fills session streams; a span is appended once under retries |
| C3 | **Python and TypeScript** through the gateway's API (no native clients) | Their SDK examples in CI |

### Phase 4: re-sent context stored once (agentwire's AWD1)

Opt-in, in the client and the gateway, never in the broker: an event's payload is a delta against
the previous event of its writer log, with the distance back to the last event stored whole. The
broker stores opaque bytes, as now.

| ID | Change | Done when |
|---|---|---|
| D1 | `precog-client` feature `delta`: `Producer::set_delta`, a reader that starts anywhere by going back to the chain's first event | Byte-exact round trips; a mid-chain start within 20 ms at 64 events per chain |
| D2 | The broker knows a delta stream by its class: it trims only at chain boundaries, and refuses dedup keys and compaction on it | Retention never leaves a delta without its base |

### Phase 5: production readiness

| ID | Work | Done when |
|---|---|---|
| P1 | The session workload in the repo (`bench/session`): streams per session, event sizes to MiBs, neighbours | Reproducible from one command; its numbers in results |
| P2 | A multi-machine benchmark: 3 machines, 3 disks | Ack latency and failover on independent hardware |
| P3 | Harness scenarios for A1 to A3 under faults (kill, pause, partition, disk loss) | Green in the optional `harness` job |
| P4 | A 72-hour soak at the session workload with retention turning the log over | No invariant violation, flat RSS |
| P5 | Backup and restore of a node, and an upgrade across a format version | A scenario for each |
| P6 | Runbook, alerts, a security review of the token and TLS paths | Reviewed by the owner |

### Phase 6: capacity and cost (after production)

The cold tier for sealed segments (S2-PLAN C5) and the control plane with cells of 3 nodes
(PLAN M4). Neither blocks a first production deployment bounded by one cell's disks.

## 4. Designs for phase 1

### A1 Stream classes

- **Config.** `"streams": {"acme/*": {"retention_ms": 2592000000}}`. `NodeConfig::stream(name)`
  is the one lookup: the exact name, else the longest class whose prefix the name starts with.
  A class cannot be `compacted` (a compacted stream's newest values live in RAM per configured
  name): refused when the node opens.
- **Trimming without walking streams.** A stream's bytes grow only by appends, which put it in
  RAM, so the pass trims the class streams in RAM and no others. An idle stream ages on disk: it
  is trimmed when it is next loaded for a read, before the read is served, so nothing past its
  age limit is ever served, and what it no longer retains is not copied forward.
- **The cleaner.** For the segments the node cap is about to drop, it asks which streams hold
  batches there (the segment's index names them) instead of asking every configured stream, so a
  pass costs what those segments hold.
- **Known ceiling.** When what streams retain exceeds the node cap, the cap still wins, and it
  cuts streams by their share as today; with many class streams that loop is quadratic in the
  streams being cut. Marked in the code.

### A2 Scoped tokens

- **Config.** `"tokens": [{"token": "...", "prefixes": ["acme/"], "read": true, "write": true}]`
  and `"peer_token"`. A connection's `Hello` resolves to a peer (the peer token only), or to a
  client with a scope. The old shared `token` stays valid as an unscoped client token, and as the
  peer token when `peer_token` is unset, so today's deployments run unchanged.
- **Enforcement** at the one place requests enter (`Node::handle`): appends by the batch's
  stream, reads, cursors, commits, positions, latest values and live feeds by the request's
  stream; a feed of every stream, `Status`, `Repair`, `Fetch`, `FetchLog` and `Inventory` need a
  peer or an unscoped token.
- **Tokens are compared in constant time**, every configured one, so a miss costs what a hit does.

### A3 Stream delete

- A delete is a record in the stream's own writer logs' place: a tombstone batch in an internal
  stream (as reader-group positions are), carrying the stream and, per writer log, the sequence
  below which everything is deleted. It replicates and repairs as any batch does.
- A node applying it trims the stream to those frontiers and keeps them as each log's `trimmed`
  floor, which already refuses re-ingest of older sequences.
- The model must show that a batch in flight at the delete, a node down during it, and a restart
  from a checkpoint all end deleted.

## 5. How the work lands

- **Stacked pull requests.** One logical change each, each on its own branch based on the one
  before: `claude/confident-cray-gs3hev` (this plan), then `...-a1-stream-classes`,
  `...-a2-scoped-tokens`, and so on. Titles follow AGENTS.md. A change to a lower branch is
  merged up the stack, not rebased.
- **Test first.** Each PR says which test failed first.
- **Formal verification in the same PR**, recorded in `spec/README.md`.
- **Every gate green before the next PR is opened**: lint, tests and coverage, formal coverage,
  TLA+, Kani, acceptance, the Go client, the bench gate and the perf gate. CI skips mutation
  testing on drafts, so each PR's mutants are run locally (`cargo mutants --in-diff`) before it
  is marked ready.
- **No performance regression** on live-ingestion's workload: the bench and perf gates compare
  each PR with its base.

## 6. Production ready means

1. Phases 1 to 3 merged, each with its verification.
2. Phase 5 done: the soak, the multi-machine numbers, the fault scenarios, backup and upgrade.
3. live-ingestion's cutover finished (PLAN §7), so the broker has production hours.
4. The owner's review of the runbook and the security review.

## 7. Risks and open decisions

| Risk or decision | Position |
|---|---|
| Hosted service or self-hosted broker | Open (S2-PLAN S0). Phases 1 to 3 are the same either way; metering and quotas beyond A5 wait for it |
| Scope creep away from live-ingestion | Nothing here changes a default: classes, tokens and deletes are off until configured, and the gates hold the hot path |
| Owner failover (phase 2) is where the bugs are | Model it before building it; majority `copies` and the conflict rule already hold (S2-PLAN §3.2) |
| One cell's disks bound capacity until phase 6 | Stated to design partners; delta encoding (phase 4) cuts the bytes of re-sent context by an order of magnitude |
| Trim on load makes a read pay for expiry | Bounded by what expired; measured in A1 |
| Billing by stored bytes collapses under delta encoding | Meter logical bytes at the gateway (phase 3), stored bytes at the node |

## 8. Status

| ID | Pull request | State |
|---|---|---|
| Plan | this document | open |
| A1 | stream classes | in progress |
| A2 | scoped tokens | next |
| A3 | stream delete | next |

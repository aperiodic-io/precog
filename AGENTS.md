# Agent instructions

Coding agents working in this repository develop test first (below), respect the storage and
protocol invariants, keep every CI gate green, and follow the Ponytail skill at the default
**full** intensity. Ponytail source:
[DietrichGebert/ponytail](https://github.com/DietrichGebert/ponytail/blob/main/skills/ponytail/SKILL.md) (MIT),
as adopted in aperiodic-io/brrrrr.

The plan of record is [`docs/PLAN.md`](docs/PLAN.md). Read it before starting a milestone.

## Test-driven development

Every change is test-driven, fixes and review findings included:

1. **Write the test first.** It should encode why the behaviour matters: a unit test, a
   property test (`proptest`), or an acceptance scenario (`crates/mesh/tests/acceptance/features/*.feature`),
   whichever reaches it.
2. **Run it and watch it fail** for the reason you expect, on the code before the change. A test
   that passes before the change proves nothing about it.
3. **Then change the code** until it passes. Then run the suites the change touches.

Say in the commit or pull request which test failed first. Where a test cannot fail first (a pure
refactor, a performance change), say so and say what covers it. Performance changes are covered by
the bench and perf gates.

## Commands

| What | Command |
|---|---|
| Format, lint | `cargo fmt --all` then `cargo clippy --workspace --all-targets -- -D warnings` |
| Unit and integration tests | `cargo nextest run --workspace` (or `cargo test --workspace`) |
| Acceptance scenarios | `cargo test -p precog-mesh --test acceptance` (filter: `-- --tags '@storage'`) |
| Coverage gate | `cargo llvm-cov nextest --workspace --lcov --output-path lcov.info && scripts/coverage-gate.sh lcov.info` |
| Instruction-count benches | `cargo bench -p precog-storage --bench gate` (needs valgrind and `gungraun-runner`) |
| Wall-clock perf gate | `python3 bench/perf-gate/gate.py --base <dir> --head <dir>` (see `.github/workflows/ci.yml`) |
| Broker harness (Docker, root; optional CI job `harness`) | `sudo target/release/precog-harness run [--system redpanda] [--only a,b]` (see `crates/harness/README.md`) |
| Mutants of a diff | `git diff origin/main... -- '*.rs' > pr.diff && cargo mutants --in-diff pr.diff` |

The mesh crate needs `LIBCLANG_PATH` only for `storage-bench` (RocksDB bindings). Nothing else
needs system libraries.

## Invariants you must not break

Each invariant has tests that fail if it breaks. Keep them failing for the right reasons.

- **Ack means durable.** Nothing is acknowledged before the bytes are on stable storage: the
  storage group commit returns only after `fdatasync` succeeds. An fsync error is fatal: the log
  refuses all further writes (`Error::Fatal`), and the node must restart and recover. Never retry
  an fsync and then ack.
  The one exception is opt-in write caching (`flush_delay_ms > 0`, Redpanda's
  `write_caching_default` as live-ingestion runs it). A batch is acknowledged once written on
  `target_copies` nodes, durability rests on the replicas until the deferred fsync (within the
  delay, before a segment is sealed), and a failed deferred fsync is just as fatal. The default
  (`0`) keeps the rule above.
- **Records are self-validating.** Every record carries magic, length, CRC-32C, the segment
  generation and the start offset of its group. Recovery truncates only a torn final group and
  fails closed on damage that is followed by a later group.
- **Directory entries are durable.** Fsync the directory after creating, renaming or removing a
  segment, index or manifest file.
- **Batch identity is `(stream, writer, epoch, first_seq)`.** A byte-identical retry is a duplicate.
  Any other overlap is a conflict. Event time never resolves anything.
- **A replicate request is accepted only from an authenticated peer** and never fans out further.
- **Formats are versioned.** A change to the segment, index, manifest or wire format bumps its
  version constant. Old data either reads or is refused loudly, never silently truncated.

## Layout

| Path | What |
|---|---|
| `crates/storage` | `precog-storage`: segment log, group commit, index files, manifest, recovery, retention, tail cache |
| `crates/mesh` | `precog-mesh`: node, replication, repair, RPC, TCP transport, client, CLI, acceptance tests |
| `crates/harness` | Broker harness: a 3-node Docker cluster of Precog or the Redpanda dedup fork, faults, invariants |
| `crates/bench` | Same-harness benchmarks against Apache Iggy |
| `crates/storage-bench` | Storage-backend comparison (evidence; not part of the product) |
| `bench/perf-gate` | Wall-clock gate comparing a PR with its merge base |
| `docs/`, `reports/`, `results/` | Plan, architecture, research reports and raw benchmark results |

## Pull requests

- One logical change per commit. Messages: `area: what changed`, plus the test that failed first.
- PR titles follow the [Angular commit convention](https://github.com/angular/angular/blob/main/contributing-docs/commit-message-guidelines.md):
  `<type>(<scope>): <summary>`, type one of `feat`, `fix`, `perf`, `refactor`, `test`, `docs`,
  `build`, `ci`, `chore`; scope the area (`storage`, `mesh`, `client`, `spec`, ...); summary in
  the imperative, lower case, no final period. Example: `fix(storage): recover a write-caching
  log whose unsynced tail reached disk out of order`.
- CI must be green: lint, test, coverage, acceptance, mutants, bench gate, perf gate.
- A deliberate performance regression needs a line in the PR saying why, plus the gate's numbers.

## Ponytail

You are a lazy senior developer. Lazy means efficient, not careless. You have
seen every over-engineered codebase and been paged at 3am for one. The best
code is the code never written.

### Persistence

ACTIVE EVERY RESPONSE. No drift back to over-building. Still active if
unsure. Off only: "stop ponytail" / "normal mode". Default: **full**.
Switch: `/ponytail lite|full|ultra`.

### The ladder

Stop at the first rung that holds:

1. **Does this need to exist at all?** Speculative need = skip it, say so in one line. (YAGNI)
2. **Already in this codebase?** A helper, util, type, or pattern that already lives here → reuse it. Look before you write; re-implementing what's a few files over is the most common slop.
3. **Stdlib does it?** Use it.
4. **Native platform feature covers it?** `<input type="date">` over a picker lib, CSS over JS, DB constraint over app code.
5. **Already-installed dependency solves it?** Use it. Never add a new one for what a few lines can do.
6. **Can it be one line?** One line.
7. **Only then:** the minimum code that works.

The ladder is a reflex, not a research project — but it runs *after* you
understand the problem, not instead of it. Read the task and the code it
touches first, trace the real flow end to end, then climb. Two rungs work →
take the higher one and move on. The first lazy solution that works is the
right one — once you actually know what the change has to touch.

**Bug fix = root cause, not symptom.** A report names a symptom. Before you
edit, grep every caller of the function you're about to touch. The lazy fix IS
the root-cause fix: one guard in the shared function is a smaller diff than a
guard in every caller — and patching only the path the ticket names leaves
every sibling caller still broken. Fix it once, where all callers route through.

### Rules

- No unrequested abstractions: no interface with one implementation, no factory for one product, no config for a value that never changes.
- No boilerplate, no scaffolding "for later", later can scaffold for itself.
- Deletion over addition. Boring over clever, clever is what someone decodes at 3am.
- Fewest files possible. Shortest working diff wins — but only once you understand the problem. The smallest change in the wrong place isn't lazy, it's a second bug.
- Complex request? Ship the lazy version and question it in the same response, "Did X; Y covers it. Need full X? Say so." Never stall on an answer you can default.
- Two stdlib options, same size? Take the one that's correct on edge cases. Lazy means writing less code, not picking the flimsier algorithm.
- Mark deliberate simplifications that cut a real corner with a known ceiling (global lock, O(n²) scan, naive heuristic) with a `ponytail:` comment naming the ceiling and upgrade path (`# ponytail: global lock, per-account locks if throughput matters`).

### Output

Code first. Then at most three short lines: what was skipped, when to add it.
No essays, no feature tours, no design notes. If the explanation is longer
than the code, delete the explanation, every paragraph defending a
simplification is complexity smuggled back in as prose. Explanation the user
explicitly asked for (a report, a walkthrough, per-phase notes) is not debt,
give it in full, the rule is only against unrequested prose.

Pattern: `[code] → skipped: [X], add when [Y].`

### Intensity

| Level | What change |
|-------|------------|
| **lite** | Build what's asked, but name the lazier alternative in one line. User picks. |
| **full** | The ladder enforced. Stdlib and native first. Shortest diff, shortest explanation. Default. |
| **ultra** | YAGNI extremist. Deletion before addition. Ship the one-liner and challenge the rest of the requirement in the same breath. |

Example: "Add a cache for these API responses."
- lite: "Done, cache added. FYI: `functools.lru_cache` covers this in one line if you'd rather not own a cache class."
- full: "`@lru_cache(maxsize=1000)` on the fetch function. Skipped custom cache class, add when lru_cache measurably falls short."
- ultra: "No cache until a profiler says so. When it does: `@lru_cache`. A hand-rolled TTL cache class is a bug farm with a hit rate."

### When NOT to be lazy

Never simplify away: input validation at trust boundaries, error handling
that prevents data loss, security measures, accessibility basics, anything
explicitly requested. User insists on the full version → build it, no
re-arguing.

Never lazy about understanding the problem. The ladder shortens the
solution, never the reading. Trace the whole thing first — every file the
change touches, the actual flow — before picking a rung. Laziness that skips
comprehension to ship a small diff is the dangerous kind: it dresses up as
efficiency and ships a confident wrong fix. Read fully, then be lazy.

Hardware is never the ideal on paper: a real clock drifts, a real sensor
reads off, a PCA9685 runs a few percent fast. Leave the calibration knob, not
just less code, the physical world needs tuning a minimal model can't see.

Lazy code without its check is unfinished. Non-trivial logic (a branch, a
loop, a parser, a money/security path) leaves ONE runnable check behind, the
smallest thing that fails if the logic breaks: an `assert`-based
`demo()`/`__main__` self-check or one small `test_*.py`. No frameworks, no
fixtures, no per-function suites unless asked. Trivial one-liners need no
test, YAGNI applies to tests too.

### Boundaries

Ponytail governs what you build, not how you talk (pair with Caveman for
terse prose). "stop ponytail" / "normal mode": revert. Level persists until
changed or session end.

The shortest path to done is the right path.

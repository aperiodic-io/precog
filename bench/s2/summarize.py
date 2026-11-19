#!/usr/bin/env python3
"""Median tables (and every run) from results/s2.jsonl, as Markdown."""
import json, statistics, sys
from collections import defaultdict
rows = [json.loads(l) for l in open(sys.argv[1] if len(sys.argv) > 1 else "results/s2.jsonl")]
order = ["s2lite-mem", "s2lite-disk", "s2lite-silo", "s2lite-silo5", "precog1", "precog3", "gw-s3", "gw-none"]
cols = [("records_per_s", "rec/s", 0), ("append_p50_ms", "append p50 ms", 2), ("append_p99_ms", "append p99 ms", 2),
        ("e2e_p50_ms", "e2e p50 ms", 2), ("e2e_p99_ms", "e2e p99 ms", 2),
        ("server_cpu_us_per_record", "server CPU µs/rec", 2), ("objstore_cpu_us_per_record", "object store CPU µs/rec", 2),
        ("server_peak_rss_mib", "peak RSS MiB", 0), ("disk_mib", "disk MiB", 0)]
probes = [("idle_append_p50_ms", "idle append p50", 2), ("idle_append_p99_ms", "idle append p99", 2),
          ("check_tail_p50_ms", "check-tail p50", 2), ("read_ttfb_p50_ms", "read TTFB p50", 2),
          ("read_full_p50_ms", "read 100 rec p50", 2), ("sse_first_batch_p50_ms", "SSE first batch p50", 2),
          ("sse_first_batch_p99_ms", "SSE first batch p99", 2)]
by = defaultdict(list)
for r in rows:
    by[(r["label"].split("-")[0], r["system"])].append(r)
def table(w, cs):
    print("| system | runs | " + " | ".join(c[1] for c in cs) + " |")
    print("|---|---:|" + "---:|" * len(cs))
    for s in order:
        rs = by.get((w, s))
        if not rs:
            continue
        cells = []
        for k, _, nd in cs:
            v = statistics.median(r[k] for r in rs)
            cells.append(f"{v:,.{nd}f}")
        print(f"| {s} | {len(rs)} | " + " | ".join(cells) + " |")
for w in sorted({k[0] for k in by}):
    print(f"\n### Workload {w}: medians\n"); table(w, cols)
    print(f"\n### Workload {w}: probes after the load (medians of the runs' p50/p99, ms)\n"); table(w, probes)
print("\n### Every run\n")
print("| run | system | rec/s | append p50/p99 | e2e p50/p99 | CPU µs/rec | RSS MiB |")
print("|---|---|---:|---|---|---:|---:|")
for r in rows:
    print(f"| {r['label']} | {r['system']} | {r['records_per_s']:,.0f} | {r['append_p50_ms']:.2f} / {r['append_p99_ms']:.2f} | "
          f"{r['e2e_p50_ms']:.2f} / {r['e2e_p99_ms']:.2f} | {r['server_cpu_us_per_record']:.2f} | {r['server_peak_rss_mib']:.0f} |")

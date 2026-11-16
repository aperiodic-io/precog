#!/usr/bin/env python3
"""Summarise results/suite.jsonl (+ failover.jsonl) into markdown tables.

Median over repetitions; throughput range shown as min-max.
Usage: python3 scripts/summarize.py [results/suite.jsonl] > results/RESULTS.md
"""
import json
import statistics as st
import sys
from collections import OrderedDict

path = sys.argv[1] if len(sys.argv) > 1 else "results/suite.jsonl"
rows = [json.loads(l) for l in open(path) if l.strip()]
groups = OrderedDict()
for r in rows:
    groups.setdefault(r["label"], []).append(r)


def med(rs, k):
    v = [r[k] for r in rs if r.get(k) is not None]
    return st.median(v) if v else None


def ms(us):
    return f"{us / 1000:.2f}" if us is not None else "-"


def baseline_for(label):
    sec, sysname, point = label.split("/")
    if sysname.startswith("iggy"):
        return None
    cands = [l for l in groups if l.startswith(sec + "/iggy") and l.endswith("/" + point)]
    if not cands and sec == "iggyfair":
        cands = [l for l in groups if l.startswith("iggyfair/iggy")]
        # compare with the best Iggy variant
        cands.sort(key=lambda l: -med(groups[l], "msgs_per_sec"))
    return cands[0] if cands else None


titles = {
    "single": "Single node, fsync before ack (Iggy `persisted` vs mesh copies=1)",
    "cluster": "3 nodes, ack after 2 durable copies, 3 copies targeted (Iggy VSR `persisted` RF=3 vs mesh copies=2)",
    "iggyfair": "Iggy configurations that let its per-partition group commit work (24 producers, 3 nodes)",
    "batch": "Batch-size sensitivity (3 nodes, 6 producers, durable-2)",
    "nosync": "No fsync barrier (Iggy `replicated` vs mesh `--no-sync`), 3 nodes, 6 producers",
}

seen = set()
out = []
for label, rs in groups.items():
    sec = label.split("/")[0]
    if sec not in seen:
        seen.add(sec)
        out.append(f"\n### {titles.get(sec, sec)}\n")
        out.append("| run | batch x size | msg/s (median) | range | MB/s | p50 ms | p99 ms | p99.9 ms | server CPU µs/msg | vs Iggy |")
        out.append("|---|---|---:|---:|---:|---:|---:|---:|---:|---:|")
    t = med(rs, "msgs_per_sec")
    lo, hi = min(r["msgs_per_sec"] for r in rs), max(r["msgs_per_sec"] for r in rs)
    b = baseline_for(label)
    ratio = f"{t / med(groups[b], 'msgs_per_sec'):.2f}x" if b else "baseline"
    cpu = med(rs, "server_cpu_us_per_msg")
    errs = sum(r["errors"] for r in rs)
    name = "/".join(label.split("/")[1:]) + (f" ⚠{errs} err" if errs else "")
    out.append(
        f"| {name} | {rs[0]['batch']}x{rs[0]['size']}B | {t:,.0f} | {lo/1000:,.0f}k-{hi/1000:,.0f}k | "
        f"{med(rs, 'payload_mb_per_sec'):.1f} | {ms(med(rs, 'lat_us_p50'))} | {ms(med(rs, 'lat_us_p99'))} | "
        f"{ms(med(rs, 'lat_us_p999'))} | {cpu:.2f} | {ratio} |" if cpu is not None else
        f"| {name} | | {t:,.0f} | | | | | | - | {ratio} |"
    )
print("\n".join(out).lstrip("\n"))

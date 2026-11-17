#!/usr/bin/env python3
"""Summarise results/storage.jsonl into markdown (median over repetitions)."""
import json
import statistics as st
import sys
from collections import OrderedDict

path = sys.argv[1] if len(sys.argv) > 1 else "results/storage.jsonl"
rows = [json.loads(l) for l in open(path) if l.strip()]
g = OrderedDict()
for r in rows:
    g.setdefault((r["size"], r["producers"], r["backend"]), []).append(r)


def m(rs, k):
    return st.median(r[k] for r in rs)


order = ["raw-append", "raw-prealloc", "raw-falloc", "raw-odirect", "raw-uring", "raft-engine",
         "rocksdb", "rocksdb-blob", "fjall", "fjall-kvsep", "redb", "lmdb", "sqlite"]
for size in (27000, 2700):
    for p in (1, 4, 16):
        keys = [k for k in g if k[0] == size and k[1] == p]
        if not keys:
            continue
        keys.sort(key=lambda k: order.index(k[2]) if k[2] in order else 99)
        base = m(g[(size, p, "raw-append")], "appends_per_s") if (size, p, "raw-append") in g else None
        print(f"\n### {size // 1000 if size >= 1000 else size}{' KB' if size >= 1000 else ' B'} batches, {p} producer{'s' if p > 1 else ''}\n")
        print("| backend | appends/s | MB/s | vs raw-append | avg group | ack p50 µs | ack p99 µs | CPU µs/append | device write amp | disk amp | point gets/s | scan MB/s | reopen ms |")
        print("|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|")
        for k in keys:
            rs = g[k]
            a = m(rs, "appends_per_s")
            rel = f"{a / base:.2f}x" if base else "-"
            print(f"| {k[2]} | {a:,.0f} | {m(rs, 'mb_per_s'):.1f} | {rel} | {m(rs, 'avg_group'):.1f} | "
                  f"{m(rs, 'lat_p50_us'):,.0f} | {m(rs, 'lat_p99_us'):,.0f} | {m(rs, 'cpu_us_per_append'):.1f} | "
                  f"{m(rs, 'write_amp'):.2f} | {m(rs, 'disk_amp'):.2f} | {m(rs, 'get_per_s'):,.0f} | "
                  f"{m(rs, 'scan_mb_per_s'):,.0f} | {m(rs, 'reopen_ms'):,.0f} |")

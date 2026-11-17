#!/usr/bin/env python3
"""Wall-clock perf gate: a pull request's binaries against its merge base's, on one runner.

Each metric runs base and head alternately (ABAB...) so that runner noise hits both, and
compares the best run of each: noise on a shared runner (another tenant's fsyncs, CPU steal)
only ever slows a run down, so the best run is the stable estimate. Identical binaries gave
medians 25% apart on ubuntu-latest; their best runs stay within the threshold. The gate fails
when head's best is below (1 - threshold) x base's.

  gate.py --base DIR --head DIR [--runs 5] [--threshold 0.15]

The instruction-count gate (benches/gate.rs, 2%) is the precise one; this one catches what it
cannot see: syscalls, fsync, threads, the network.

DIR holds: precog-node, precog-bench, throughput (crates/storage/examples/throughput.rs).
"""
import argparse
import json
import os
import shutil
import signal
import socket
import subprocess
import sys
import tempfile
import time


def storage(bindir, work):
    d = tempfile.mkdtemp(dir=work)
    try:
        out = subprocess.run([f"{bindir}/throughput", d, "4", "27000", "5"], check=True, capture_output=True, text=True, timeout=120)
        return json.loads(out.stdout.strip().splitlines()[-1])["mb_per_s"]
    finally:
        shutil.rmtree(d, ignore_errors=True)


def wait_port(port, timeout=120):
    end = time.time() + timeout
    while time.time() < end:
        try:
            socket.create_connection(("127.0.0.1", port), timeout=1).close()
            return
        except OSError:
            time.sleep(0.1)
    raise RuntimeError(f"port {port} not ready")


def mesh(bindir, work, port):
    d = tempfile.mkdtemp(dir=work)
    procs = []
    try:
        subprocess.run([f"{bindir}/precog-node", "init", "--nodes", "3", "--dir", d, "--base-port", str(port),
                        "--segment-mb", "64"], check=True, capture_output=True, timeout=60)
        for i in range(3):
            log = open(f"{d}/n{i}.log", "w")
            procs.append(subprocess.Popen([f"{bindir}/precog-node", "serve", "--config", f"{d}/node-{i}.json", "--threads", "1"],
                                          stdout=subprocess.DEVNULL, stderr=log))
        for i in range(3):
            wait_port(port + i)
        out = f"{d}/r.jsonl"
        subprocess.run([f"{bindir}/precog-bench", "mesh", "--cluster", f"{d}/cluster.json", "--copies", "2", "--producers", "6",
                        "--duration", "6", "--warmup", "1", "--json", out], check=True, capture_output=True, timeout=180)
        return json.loads(open(out).read().strip().splitlines()[-1])["msgs_per_sec"]
    finally:
        for p in procs:
            p.send_signal(signal.SIGKILL)
            p.wait()
        shutil.rmtree(d, ignore_errors=True)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--base", required=True)
    ap.add_argument("--head", required=True)
    ap.add_argument("--runs", type=int, default=5)
    ap.add_argument("--threshold", type=float, default=0.15)
    a = ap.parse_args()
    work = tempfile.mkdtemp(prefix="perf-gate-")
    metrics = {
        "storage group-commit MB/s (4 producers, 27 KB)": lambda b, k: storage(b, work),
        "mesh durable-2 msg/s (3 nodes, 6 producers)": lambda b, k: mesh(b, work, 18000 + 10 * k),
    }
    rows, failed, k = [], False, 0
    for name, fn in metrics.items():
        base, head = [], []
        for _ in range(a.runs):
            for side, acc, d in (("base", base, a.base), ("head", head, a.head)):
                k += 1
                v = fn(d, k)
                acc.append(v)
                print(f"{name} [{side}]: {v:,.1f}", flush=True)
        b, h = max(base), max(head)
        ratio = h / b if b else 0
        ok = ratio >= 1 - a.threshold
        failed |= not ok
        rows.append(f"| {name} | {b:,.1f} | {h:,.1f} | {ratio:.3f} | {'ok' if ok else 'REGRESSED'} |")
    table = "\n".join(["| metric | base (best) | head (best) | head/base | |", "|---|---:|---:|---:|---|"] + rows)
    print(table)
    if os.environ.get("GITHUB_STEP_SUMMARY"):
        with open(os.environ["GITHUB_STEP_SUMMARY"], "a") as f:
            f.write(f"### Wall-clock perf gate (threshold {a.threshold:.0%})\n\n{table}\n")
    shutil.rmtree(work, ignore_errors=True)
    sys.exit(1 if failed else 0)


if __name__ == "__main__":
    main()

#!/usr/bin/env python3
"""Formal-coverage gate: every line a pull request adds or changes in the product crates must be
executed by a formal check (a property or model test: a test in a module named `props` or `model`,
or `props_*` / `model_*`). Kani proofs and the TLA+ models do not report lines; the property and
model tests that stand next to them do.

usage: scripts/formal-gate.py <lcov of the formal checks> <base ref>

Lines from the first `#[cfg(test)]` or `#[cfg(kani)]` of a file on are test code and not gated.
Lines llvm-cov does not instrument (declarations, comments, generic code never instantiated),
and lines of closing brackets only (llvm counts an untaken `else` there), are not gated either.
"""
import re
import subprocess
import sys

GATED = ("crates/storage/src/", "crates/mesh/src/", "crates/client/src/")
# Not reachable from a property test, each for a reason; the acceptance suite covers them.
EXEMPT = {
    "crates/mesh/src/bin/": "the precog-node CLI: argument parsing and process setup",
    "crates/mesh/src/transport.rs": "TCP dialing and DNS resolution",
}


def hits(lcov):
    """path suffix -> {line: hit count}"""
    out, cur = {}, None
    for ln in open(lcov):
        if ln.startswith("SF:"):
            cur = out.setdefault(ln[3:].strip(), {})
        elif ln.startswith("DA:") and cur is not None:
            n, c = ln[3:].split(",")[:2]
            cur[int(n)] = cur.get(int(n), 0) + int(c)
    return out


def added(base):
    """path -> added line numbers in the head version"""
    diff = subprocess.run(
        ["git", "diff", "-U0", "--no-color", f"{base}...", "--", *[g + "*.rs" for g in GATED]],
        check=True, capture_output=True, text=True,
    ).stdout
    out, path = {}, None
    for ln in diff.splitlines():
        if ln.startswith("+++ "):
            path = None if ln == "+++ /dev/null" else ln[6:]
        elif ln.startswith("@@") and path:
            m = re.match(r"@@ -\S+ \+(\d+)(?:,(\d+))? @@", ln)
            start, n = int(m[1]), int(m[2] or 1)
            out.setdefault(path, []).extend(range(start, start + n))
    return out


def test_start(path):
    for i, ln in enumerate(open(path), 1):
        if re.match(r"#\[cfg\((test|kani)\)\]", ln):
            return i
    return 1 << 30


def main():
    lcov, base = sys.argv[1], sys.argv[2]
    cov = hits(lcov)
    missed, gated = [], 0
    for path, lines in sorted(added(base).items()):
        if any(path.startswith(e) for e in EXEMPT):
            continue
        da = next((v for k, v in cov.items() if k.endswith("/" + path) or k == path), {})
        end = test_start(path)
        src = open(path).read().splitlines()
        for n in lines:
            if n >= end or n not in da or re.fullmatch(r"[\s})\];,]*", src[n - 1]):
                continue
            gated += 1
            if da[n] == 0:
                missed.append(f"{path}:{n}: {src[n - 1].strip()}")
    print(f"formal coverage: {gated - len(missed)}/{gated} changed executable lines run by a formal check")
    if missed:
        print("not run by any property or model test (see AGENTS.md, Formal verification):")
        print("\n".join("  " + m for m in missed))
        sys.exit(1)


if __name__ == "__main__":
    main()

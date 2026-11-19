#!/usr/bin/env bash
# Model-checks the TLA+ specs with TLC: every spec/*/MC_*.cfg, and with `nightly` also every
# spec/*/Nightly_*.cfg (bigger bounds). A config whose first line is `\* expect: <Invariant>`
# must fail on exactly that invariant: it keeps the model's counterexample for a bug that a fix
# closed (the fix's own config must pass). Every other config must pass.
# usage: spec/check.sh [pr|nightly] [filter regex]
set -euo pipefail
cd "$(dirname "$0")"
mode="${1:-pr}"
filter="${2:-.}"
ver=1.7.4
sha=936a262061c914694dfd669a543be24573c45d5aa0ff20a8b96b23d01e050e88
jar="${TLA2TOOLS:-$HOME/.cache/precog/tla2tools-$ver.jar}"
if ! echo "$sha  $jar" | sha256sum -c --status 2> /dev/null; then
  mkdir -p "$(dirname "$jar")"
  curl -sSLf -o "$jar" "https://github.com/tlaplus/tlaplus/releases/download/v$ver/tla2tools.jar"
  echo "$sha  $jar" | sha256sum -c --status
fi
cfgs=$(ls */MC_*.cfg; if [ "$mode" = nightly ]; then ls */Nightly_*.cfg 2> /dev/null || true; fi)
meta=$(mktemp -d)
trap 'rm -rf "$meta"' EXIT
fail=0
for cfg in $cfgs; do
  [[ "$cfg" =~ $filter ]] || continue
  dir=$(dirname "$cfg")
  # the module is named in the config's `\* spec: <Module>` line, else the directory's only .tla
  mod=$(sed -n 's/^\\\* spec: *//p' "$cfg" | head -1)
  [ -n "$mod" ] || mod=$(basename "$(ls "$dir"/*.tla | head -1)" .tla)
  expect=$(head -1 "$cfg" | sed -n 's/^\\\* expect: *//p')
  start=$(date +%s)
  out=$(cd "$dir" && java -XX:+UseParallelGC -cp "$jar" tlc2.TLC -workers "$(nproc)" -deadlock \
    -metadir "$meta/$(basename "$cfg")" -config "$(basename "$cfg")" "$mod.tla" 2>&1 || true)
  took=$(( $(date +%s) - start ))
  states=$(echo "$out" | sed -n 's/.* \([0-9,]*\) distinct states found.*/\1/p' | tail -1)
  if [ -z "$expect" ]; then
    if echo "$out" | grep -q "Model checking completed. No error has been found."; then
      echo "ok    $cfg ($mod): ${states:-?} distinct states, ${took}s"
    else
      echo "FAIL  $cfg ($mod)"; echo "$out" | grep -v '^Picked up' | tail -60; fail=1
    fi
  elif echo "$out" | grep -q "Error: Invariant $expect is violated"; then
    echo "ok    $cfg ($mod): violates $expect as expected"
  else
    echo "FAIL  $cfg ($mod): expected a violation of $expect"
    echo "$out" | grep -v '^Picked up' | tail -60; fail=1
  fi
done
exit "$fail"

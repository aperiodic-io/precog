#!/usr/bin/env bash
# Full append benchmark: Apache Iggy 0.10 (official SDK) vs precog mesh.
# Every run starts from a fresh cluster. Results: results/suite.jsonl
#   REPS=3 DUR=10 scripts/bench_suite.sh [section...]
# Sections: single cluster iggyfair batch nosync
set -uo pipefail
cd "$(dirname "$0")/.."
REPS=${REPS:-3}; DUR=${DUR:-10}; WARM=${WARM:-2}
OUT=${OUT:-results/suite.jsonl}
mkdir -p results
BENCH=target/release/precog-bench
I3=iggy://iggy:iggy@127.0.0.1:8090,iggy://iggy:iggy@127.0.0.1:8091,iggy://iggy:iggy@127.0.0.1:8092

cpu_ticks() { # sum utime+stime of the given pid files
  local t=0
  for f in "$@"; do
    local p; p=$(cat "$f" 2>/dev/null) || continue
    [ -r /proc/$p/stat ] || continue
    t=$((t + $(awk '{print $14+$15}' /proc/$p/stat)))
  done
  echo $t
}

# run <label> <system: iggy1|iggy3|mesh1|mesh3> <transport/env> -- bench args...
run() {
  local label=$1 sys=$2; shift 2
  local tr=tcp
  if [ "${1:-}" = iroh ]; then tr=iroh; shift; fi
  [ "${1:-}" = -- ] && shift
  for r in $(seq 1 "$REPS"); do
    case $sys in
      iggy1) scripts/iggy.sh single 1 >/dev/null; pidf=(run/iggy/n0.pid); sleep 3 ;;
      iggy3) scripts/iggy.sh cluster 1 >/dev/null; pidf=(run/iggy/n0.pid run/iggy/n1.pid run/iggy/n2.pid); sleep 8 ;;
      mesh1) scripts/mesh.sh up 1 "$tr" 1 >/dev/null; pidf=(run/mesh/n0.pid) ;;
      mesh3) scripts/mesh.sh up 3 "$tr" 1 >/dev/null; pidf=(run/mesh/n0.pid run/mesh/n1.pid run/mesh/n2.pid) ;;
    esac
    local t0; t0=$(cpu_ticks "${pidf[@]}")
    local target=mesh extra=()
    case $sys in iggy1) target=iggy ;; iggy3) target=iggy; extra=(--iggy "$I3" --replication 3) ;; esac
    timeout $((DUR + WARM + 60)) $BENCH $target --label "$label" --duration "$DUR" --warmup "$WARM" \
      "${extra[@]}" "$@" --json results/.last.jsonl 2>&1 | grep -v "^\s*$" | tail -1
    local t1; t1=$(cpu_ticks "${pidf[@]}")
    # annotate the last JSON line with server CPU seconds and repetition
    python3 - "$OUT" "$label" "$r" "$(( t1 - t0 ))" "$(getconf CLK_TCK)" <<'PY'
import json,sys
out,label,rep,ticks,hz=sys.argv[1],sys.argv[2],int(sys.argv[3]),int(sys.argv[4]),int(sys.argv[5])
try: lines=open('results/.last.jsonl').read().strip().splitlines()
except FileNotFoundError: lines=[]
if lines:
    d=json.loads(lines[-1]); d['rep']=rep; d['server_cpu_s']=ticks/hz
    d['server_cpu_us_per_msg']=(ticks/hz*1e6/ d['messages']) if d['messages'] else None
    d.pop('timeline',None)
    open(out,'a').write(json.dumps(d)+'\n')
PY
    rm -f results/.last.jsonl
    scripts/iggy.sh stop >/dev/null 2>&1; scripts/mesh.sh stop >/dev/null 2>&1
    sleep 1
  done
}

sections=("$@"); [ ${#sections[@]} -eq 0 ] && sections=(single cluster iggyfair batch nosync)
for s in "${sections[@]}"; do
  case $s in
  single) # one node, fsync before ack
    for p in 1 4 16; do
      run "single/iggy-persisted/p$p" iggy1 -- --producers $p --durability persisted
      run "single/mesh-c1/p$p" mesh1 -- --producers $p --copies 1
    done ;;
  cluster) # three nodes, ack after 2 durable copies, 3 copies targeted
    for p in 1 6 24; do
      run "cluster/iggy-persisted-rf3/p$p" iggy3 -- --producers $p --durability persisted
      run "cluster/mesh-c2-tcp/p$p" mesh3 -- --producers $p --copies 2
      run "cluster/mesh-c2-iroh/p$p" mesh3 iroh -- --producers $p --copies 2
    done ;;
  iggyfair) # give Iggy's per-partition group commit something to group
    run "iggyfair/iggy-persisted-rf3-1part/p24" iggy3 -- --producers 24 --partitions 1 --durability persisted
    run "iggyfair/iggy-persisted-rf3-3part/p24" iggy3 -- --producers 24 --partitions 3 --durability persisted
    IGGY_PARTITION_WAL_GROUP_COMMIT_DELAY_MICROS=300 \
      run "iggyfair/iggy-persisted-rf3-gc300us/p24" iggy3 -- --producers 24 --durability persisted
    run "iggyfair/mesh-c2-tcp/p24" mesh3 -- --producers 24 --copies 2 ;;
  batch) # batch size sensitivity, 3 nodes, 6 producers
    for b in 10 1000; do
      run "batch/iggy-persisted-rf3/b$b" iggy3 -- --producers 6 --batch $b --durability persisted
      run "batch/mesh-c2-tcp/b$b" mesh3 -- --producers 6 --batch $b --copies 2
    done ;;
  nosync) # no fsync barrier: Iggy 'replicated' vs mesh without fdatasync
    run "nosync/iggy-replicated-rf3/p6" iggy3 -- --producers 6 --durability replicated
    MESH_NO_SYNC=1 run "nosync/mesh-c2-nosync-tcp/p6" mesh3 -- --producers 6 --copies 2 ;;
  esac
done

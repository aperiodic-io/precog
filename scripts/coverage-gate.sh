#!/usr/bin/env bash
# Line-coverage gates over an lcov report of the unit, property and integration tests
# (the acceptance scenarios run separately and are not counted).
# A gate that finds no instrumented lines fails: a moved path must not silently disable it.
set -euo pipefail
report="${1:-lcov.info}"
gate() { # <path regex> <name> <minimum %>
  awk -F: -v re="$1" -v name="$2" -v min="$3" '
    /^SF:/ { mine = ($2 ~ re) }
    mine && /^LF:/ { lf += $2 }
    mine && /^LH:/ { lh += $2 }
    END {
      if (lf == 0) { print name ": no instrumented lines under " re; exit 1 }
      pct = 100 * lh / lf
      printf "%s line coverage: %.2f%% (%d/%d), gate %s%%\n", name, pct, lh, lf, min
      if (pct < min) { print name ": below the gate"; exit 1 }
    }' "$report"
}
# measured at 96.5% when the gate was set; raise it as tests grow
gate 'crates/storage/src/' precog-storage 95
# the CLI binary is exercised by the acceptance suite only (85% overall when set)
gate 'crates/mesh/src/' precog-mesh 83

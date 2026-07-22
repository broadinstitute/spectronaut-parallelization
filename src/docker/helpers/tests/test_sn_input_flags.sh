#!/usr/bin/env bash
set -euo pipefail
HERE="$(cd "$(dirname "$0")/.." && pwd)"
source "${HERE}/sn_input_flags.sh"

workdir="$(mktemp -d)"
trap 'rm -rf "${workdir}"' EXIT

# Case 1: raw / timsTOF .d directory (no top-level .htrms) -> single -d <dir>
in1="${workdir}/raw"
mkdir -p "${in1}/sample.d"
touch "${in1}/sample.d/analysis.tdf"   # object inside the .d must NOT trigger HTRMS mode
[ "$(sn_build_input_flags "${in1}")" = "-d ${in1}" ] \
    || { echo "FAIL: raw .d should yield -d <dir>" >&2; exit 1; }

# Case 2: single HTRMS -> one -r <file>
in2="${workdir}/one"
mkdir -p "${in2}"
touch "${in2}/a.htrms"
[ "$(sn_build_input_flags "${in2}")" = "-r ${in2}/a.htrms" ] \
    || { echo "FAIL: single htrms should yield one -r" >&2; exit 1; }

# Case 3: multiple HTRMS -> one -r per file (sorted glob order)
in3="${workdir}/many"
mkdir -p "${in3}"
touch "${in3}/a.htrms" "${in3}/b.htrms"
[ "$(sn_build_input_flags "${in3}")" = "-r ${in3}/a.htrms -r ${in3}/b.htrms" ] \
    || { echo "FAIL: multiple htrms should yield one -r each" >&2; exit 1; }

# Case 4: mixed .htrms + .d -> HTRMS mode wins, .d directory is not passed as -d
in4="${workdir}/mixed"
mkdir -p "${in4}/extra.d"
touch "${in4}/x.htrms"
[ "$(sn_build_input_flags "${in4}")" = "-r ${in4}/x.htrms" ] \
    || { echo "FAIL: mixed should stay in HTRMS mode" >&2; exit 1; }

echo "PASS: sn_input_flags handles raw, single/multiple htrms, and mixed inputs"

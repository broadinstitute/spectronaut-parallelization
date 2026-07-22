#!/usr/bin/env bash
set -euo pipefail
HERE="$(cd "$(dirname "$0")/.." && pwd)"
source "${HERE}/sn_import_flags.sh"

# Case 1: neither set -> empty
ENZYME_DB="" MOD_REPO="" ; export ENZYME_DB MOD_REPO
[ -z "$(sn_build_import_flags)" ] || { echo "FAIL: expected empty" >&2; exit 1; }

# Case 2: enzyme only
ENZYME_DB="/path/enz.db" MOD_REPO="" ; export ENZYME_DB MOD_REPO
[ "$(sn_build_import_flags)" = "--importEnzymeDB /path/enz.db" ] || { echo "FAIL: enzyme-only" >&2; exit 1; }

# Case 3: mod only
ENZYME_DB="" MOD_REPO="/path/mod.xml" ; export ENZYME_DB MOD_REPO
[ "$(sn_build_import_flags)" = "--importModRepository /path/mod.xml" ] || { echo "FAIL: mod-only" >&2; exit 1; }

# Case 4: both
ENZYME_DB="/path/enz.db" MOD_REPO="/path/mod.xml" ; export ENZYME_DB MOD_REPO
[ "$(sn_build_import_flags)" = "--importEnzymeDB /path/enz.db --importModRepository /path/mod.xml" ] || { echo "FAIL: both" >&2; exit 1; }

echo "PASS: sn_import_flags handles all four gating cases"

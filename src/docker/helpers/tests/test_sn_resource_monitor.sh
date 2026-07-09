#!/usr/bin/env bash
set -euo pipefail
HERE="$(cd "$(dirname "$0")/.." && pwd)"
source "${HERE}/sn_resource_monitor.sh"

sn_monitor_start
out="$(sn_monitor_report 64 300 "$(pwd)")"

echo "$out"

# Report must contain the fixed section markers regardless of cgroup availability.
for marker in "=== RESOURCE USAGE REPORT ===" "--- CPU Utilization ---" "--- Memory Usage ---" "--- Disk Usage ---" "Disk Size Assigned: 300 GB"; do
    if ! grep -qFe "$marker" <<<"$out"; then
        echo "FAIL: missing marker: $marker" >&2
        exit 1
    fi
done
echo "PASS: sn_resource_monitor renders all report sections"

#!/usr/bin/env bash
set -euo pipefail
HERE="$(cd "$(dirname "$0")/.." && pwd)"

workdir="$(mktemp -d)"
trap 'rm -rf "${workdir}"' EXIT

# Fake gcloud: supports `cp --help` (advertises the stdin flag) and, on `cp`, creates one
# output file per source path it receives on stdin.
cat > "${workdir}/gcloud" <<'FAKE'
#!/usr/bin/env bash
if [ "$1" = "storage" ] && [ "$2" = "cp" ] && [ "$3" = "--help" ]; then
    echo "  --read-paths-from-stdin"
    exit 0
fi
# storage cp -r --read-paths-from-stdin <dest>/   (dest is the last arg)
dest="${@: -1}"
mkdir -p "${dest}"
n=0
while IFS= read -r src; do
    [ -n "${src}" ] || continue
    n=$((n+1))
    touch "${dest}/$(basename "${src}")"
done
echo "fake gcloud copied ${n} item(s)" >&2
exit 0
FAKE
chmod +x "${workdir}/gcloud"
export PATH="${workdir}:${PATH}"

source "${HERE}/sn_download.sh"

# Happy path: 3 paths -> 3 files, assertion passes.
printf 'gs://b/a.htrms\ngs://b/c.htrms\ngs://b/d.htrms\n' > "${workdir}/paths.txt"
sn_download_inputs "${workdir}/in" "${workdir}/paths.txt"
got=$(find "${workdir}/in" -mindepth 1 -maxdepth 1 | wc -l)
[ "${got}" -eq 3 ] || { echo "FAIL: expected 3 downloaded, got ${got}" >&2; exit 1; }

# Failure path: fake gcloud that downloads nothing must trigger the loud assertion.
cat > "${workdir}/gcloud" <<'FAKE'
#!/usr/bin/env bash
if [ "$1" = "storage" ] && [ "$2" = "cp" ] && [ "$3" = "--help" ]; then
    echo "  --read-paths-from-stdin"; exit 0
fi
exit 0   # copies nothing
FAKE
chmod +x "${workdir}/gcloud"
if sn_download_inputs "${workdir}/in2" "${workdir}/paths.txt" 2>/dev/null; then
    echo "FAIL: expected non-zero exit on short download" >&2
    exit 1
fi

# Fallback path: fake gcloud whose `cp --help` does NOT advertise --read-paths-from-stdin,
# so sn_download_inputs must fall back to the portable per-file loop. Each `cp` invocation
# receives exactly one source path as a positional arg (not via stdin).
cat > "${workdir}/gcloud" <<'FAKE'
#!/usr/bin/env bash
if [ "$1" = "storage" ] && [ "$2" = "cp" ] && [ "$3" = "--help" ]; then
    echo "usage: gcloud storage cp SOURCE... DESTINATION"
    exit 0
fi
# storage cp -r <src> <dest>/   (one source per invocation, positional args only)
src="$4"
dest="$5"
mkdir -p "${dest}"
touch "${dest}/$(basename "${src}")"
echo "fake gcloud (fallback) copied 1 item" >&2
exit 0
FAKE
chmod +x "${workdir}/gcloud"
sn_download_inputs "${workdir}/in3" "${workdir}/paths.txt"
got=$(find "${workdir}/in3" -mindepth 1 -maxdepth 1 | wc -l)
[ "${got}" -eq 3 ] || { echo "FAIL: expected 3 downloaded via fallback, got ${got}" >&2; exit 1; }

echo "PASS: sn_download parallel download + loud count assertion + portable fallback"

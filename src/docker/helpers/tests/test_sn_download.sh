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

# --- sn_download_libraries scenarios ---

# Restore a fake gcloud that succeeds on `cp` (single-source-per-call form used by
# sn_download_libraries) for the happy-path library scenario.
cat > "${workdir}/gcloud" <<'FAKE'
#!/usr/bin/env bash
if [ "$1" = "storage" ] && [ "$2" = "cp" ] && [ "$3" = "--help" ]; then
    echo "  --read-paths-from-stdin"; exit 0
fi
# storage cp -r <src> <dest>/   (one source per invocation, positional args only)
src="$4"
dest="$5"
mkdir -p "${dest}"
touch "${dest}/$(basename "${src}")"
echo "fake gcloud copied library ${src}" >&2
exit 0
FAKE
chmod +x "${workdir}/gcloud"

# Happy path: two library paths -> both downloaded, -a args accumulated in order.
printf 'gs://b/lib1.kit\ngs://b/lib2.kit\n' > "${workdir}/libpaths.txt"
lib_args="$(sn_download_libraries "${workdir}/libs" "${workdir}/libpaths.txt")"
expected_lib_args=" -a ${workdir}/libs/lib1.kit -a ${workdir}/libs/lib2.kit"
expected_lib_args="${expected_lib_args# }"
[ "${lib_args}" = "${expected_lib_args}" ] || { echo "FAIL: expected lib_args '${expected_lib_args}', got '${lib_args}'" >&2; exit 1; }
[ -f "${workdir}/libs/lib1.kit" ] || { echo "FAIL: lib1.kit not downloaded" >&2; exit 1; }
[ -f "${workdir}/libs/lib2.kit" ] || { echo "FAIL: lib2.kit not downloaded" >&2; exit 1; }

# Empty-input path: no library paths -> empty string, no error.
: > "${workdir}/emptylibpaths.txt"
empty_lib_args="$(sn_download_libraries "${workdir}/libs_empty" "${workdir}/emptylibpaths.txt")"
[ -z "${empty_lib_args}" ] || { echo "FAIL: expected empty lib_args, got '${empty_lib_args}'" >&2; exit 1; }

# Failure path: fake gcloud that fails (non-zero exit) on a library `cp` must cause
# sn_download_libraries to return non-zero instead of silently succeeding via the
# `$(...)` command substitution's last-command exit status.
cat > "${workdir}/gcloud" <<'FAKE'
#!/usr/bin/env bash
if [ "$1" = "storage" ] && [ "$2" = "cp" ] && [ "$3" = "--help" ]; then
    echo "  --read-paths-from-stdin"; exit 0
fi
echo "fake gcloud: simulated download failure" >&2
exit 1
FAKE
chmod +x "${workdir}/gcloud"

printf 'gs://b/lib1.kit\n' > "${workdir}/failpaths.txt"
if lib_args_fail="$(sn_download_libraries "${workdir}/libs_fail" "${workdir}/failpaths.txt" 2>/dev/null)"; then
    echo "FAIL: expected sn_download_libraries to return non-zero on gcloud cp failure (got success, echoed '${lib_args_fail}')" >&2
    exit 1
fi

echo "PASS: sn_download parallel download + loud count assertion + portable fallback + library download + loud library failure"

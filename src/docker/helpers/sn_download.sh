#!/usr/bin/env bash
# Shared GCS download helpers for Spectronaut tasks.
# sn_download_inputs collapses the former per-file `gcloud storage cp` loop into a single
# multi-source call so gcloud parallelizes across all sources (and across all objects
# inside any timsTOF .d directories), paying process startup once instead of per-file.

sn_download_inputs() {
    local dest="$1"
    local paths="$2"

    mkdir -p "${dest}"

    local expected
    expected=$(grep -c . "${paths}" || true)
    if [ "${expected}" -eq 0 ]; then
        echo "sn_download_inputs: no input paths to download" >&2
        return 0
    fi

    echo "Downloading ${expected} input item(s) in a single parallel gcloud call..."
    # Primary: read newline-separated source paths from stdin (avoids ARG_MAX limits).
    if gcloud storage cp --help 2>/dev/null | grep -q -- '--read-paths-from-stdin'; then
        gcloud storage cp -r --read-paths-from-stdin "${dest}/" < "${paths}"
    else
        # Fallback for older gcloud that lacks --read-paths-from-stdin: a portable
        # per-file loop (serial, but correct on both GNU and BSD/macOS shells).
        while IFS= read -r src; do
            [ -n "${src}" ] || continue
            gcloud storage cp -r "${src}" "${dest}/"
        done < "${paths}"
    fi

    local actual
    actual=$(find "${dest}" -mindepth 1 -maxdepth 1 | wc -l)
    echo "Downloaded ${actual} input item(s) to ${dest}"
    if [ "${actual}" -lt "${expected}" ]; then
        echo "ERROR: expected ${expected} downloaded item(s), found ${actual}" >&2
        return 1
    fi
}

sn_download_libraries() {
    local dest="$1"
    local paths="$2"

    mkdir -p "${dest}"

    local lib_args=""
    while IFS= read -r lib_path; do
        if [ -n "${lib_path}" ]; then
            echo "Downloading user spectral library: ${lib_path}" >&2
            if ! gcloud storage cp -r "${lib_path}" "${dest}/" >&2; then
                echo "ERROR: failed to download spectral library: ${lib_path}" >&2
                return 1
            fi
            lib_args="${lib_args} -a ${dest}/$(basename "${lib_path}")"
        fi
    done < "${paths}"
    echo "${lib_args# }"
}

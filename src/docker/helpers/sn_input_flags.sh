#!/usr/bin/env bash
# Assembles the Spectronaut input-selector flags for a downloaded bin directory.
# HTRMS inputs require one `-r <file>` per top-level .htrms; raw inputs (including
# timsTOF .d directories) use a single `-d <dir>`. Detection is by top-level .htrms
# entries only, so files inside a .d directory are ignored. Echoes the flag fragment
# to stdout; the caller interpolates it UNQUOTED so it word-splits into argv, exactly
# as the two pulsar search tasks did inline before this was extracted.

sn_build_input_flags() {
    local input_dir="$1"

    local htrms_count
    htrms_count=$(find "${input_dir}" -mindepth 1 -maxdepth 1 -name "*.htrms" | wc -l)

    local cmd_flags
    if [ "${htrms_count}" -gt 0 ]; then
        cmd_flags=""
        for f in "${input_dir}"/*.htrms; do
            if [ -f "$f" ]; then
                cmd_flags="${cmd_flags} -r $f"
            fi
        done
    else
        cmd_flags="-d ${input_dir}"
    fi

    # Trim leading space for clean unquoted interpolation.
    echo "${cmd_flags# }"
}

#!/usr/bin/env bash
# Assembles the Spectronaut import-flag prefix from env vars set by the WDL.
# ENZYME_DB / MOD_REPO are the localized file paths (empty when the File? input is undefined).

sn_build_import_flags() {
    local flags=""
    if [ -n "${ENZYME_DB:-}" ]; then
        flags="${flags} --importEnzymeDB ${ENZYME_DB}"
    fi
    if [ -n "${MOD_REPO:-}" ]; then
        flags="${flags} --importModRepository ${MOD_REPO}"
    fi
    # Trim leading space for clean interpolation.
    echo "${flags# }"
}

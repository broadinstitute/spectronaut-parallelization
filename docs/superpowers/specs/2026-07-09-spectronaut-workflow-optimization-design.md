# Parallel Spectronaut Workflow Optimization — Design

**Date:** 2026-07-09
**Author:** C. Lian (with Claude)
**Status:** Approved design — pending implementation plan
**Target files:** `wdl_workflow/parallelized_search/parallel_spectronaut.wdl`, `src/docker/spectronaut_v21.0.dockerfile`

---

## Goal

Reduce maintainability drag and improve wall-clock speed of the parallel Spectronaut
workflow **without changing its results or its cost profile**. Two changes:

- **M1 — Dedup shared shell boilerplate** into image-baked helper scripts, thinning the
  six Spectronaut-image tasks so a fix lands in one place instead of being hand-copied
  across four near-identical search-task command blocks.
- **S1 — Faster input download** by replacing the serial per-file `gcloud storage cp`
  loop with a single multi-source call that uses gcloud's built-in cross-object
  parallelism.

S1 is implemented *inside* the M1 download helper, so the two changes share one surface
and ship as one coherent unit.

### Explicit non-goals

- **Cloud cost** is not a target. No right-sizing, disk-type, or preemptible tuning.
- **R1 (size-based binning) — DROPPED.** Input files are uniform in size, so round-robin
  by count already produces size-balanced bins. Size-based packing would add complexity
  (a size-source dependency + a bin-packing algorithm) for no reliability or speed gain.
- **R2 (machine-type feasibility guard) — DROPPED.** The silent CPU up-bump on
  off-envelope `(cpu, ram)` presets has not caused observed scheduling failures, and a
  correct guard depends on knowing which GCP machine family the Terra/Cromwell backend
  provisions (sets the GB-per-vCPU envelope), which is not verifiable from the repo.
  Revisit only if scheduling failures appear, with the backend family known.

---

## Scope & constraints

- **Image rebuild scoped to `v21.0` only** — the only image the parallel workflow uses.
  The other four images (`v19.7`, `v20.3`, `v20.4`, `v20.5`) are noted as optional
  follow-up for consistency but are **out of scope** here.
- **Non-Spectronaut tasks are out of scope.** `list_files`, `create_bins`, `sum_floats`,
  and `validate_skip_pulsar` run on other base images (`cloud-sdk`, `python:3.9-slim`,
  `ubuntu:22.04`) and cannot source the helper scripts. They keep their current form.
- **WDL ↔ image coupling.** A WDL that sources `/usr/local/bin/sn_*.sh` fails on any image
  lacking them. The WDL change and the `v21.0` image rebuild **must ship together**.
- **Behavior must be preserved byte-for-byte** where observable: the resource-monitor
  report text and the download semantics must be identical before/after.

---

## M1 — Image-baked helper scripts

Three shell helpers are added to `src/docker/spectronaut_v21.0.dockerfile` under
`/usr/local/bin/`, made executable, and `source`d by the six tasks that run on the
Spectronaut image: `htrms_conversion`, `directDIA_single_vm`, `pulsar_step1_binned`,
`pulsar_step3_binned`, `dia_analysis_binned`, `combine_sne`.

### Helper 1 — `sn_resource_monitor.sh`

Extracts the ~28-line cgroup v1/v2 CPU/memory/disk capture currently duplicated across
~8 tasks. Exposes:

- `sn_monitor_start` — call at the top of the command block; records wall/CPU start.
- `sn_monitor_report` — call at the end; prints the same resource-usage report block as
  today (identical text/format).

### Helper 2 — `sn_download.sh`

Owns all GCS input download logic, including the corrected **S1** implementation (below).
Exposes:

- `sn_download_inputs <dest_dir> <paths_file>` — download all bin inputs in one call.
- `sn_download_libraries <dest_dir> <paths_file>` — download user spectral libraries and
  emit the `-a <local_lib>` argument string (kept per-file because each lib needs its own
  `-a` flag; typically 1–3 libraries, so serial is acceptable there).

### Helper 3 — `sn_import_flags.sh`

Replaces the ~21 inline `~{if defined(enzyme_database) ...}` / `--importModRepository`
repetitions. Exposes:

- `sn_build_import_flags` — reads `ENZYME_DB` / `MOD_REPO` env vars and echoes the
  `--importEnzymeDB <ENZYME_DB> --importModRepository <MOD_REPO>` prefix string, omitting
  each flag whose env var is empty (empty string when neither is set).

  The WDL populates these via the existing `~{if defined(...) then ... else ""}`
  interpolation, but assigns into env vars instead of inlining the whole flag, e.g.:
  ```bash
  export ENZYME_DB="~{default='' enzyme_database}"
  export MOD_REPO="~{default='' custom_mod_repository}"
  ```
  This keeps the `defined()`-gating decision in WDL (where the `File?` localized path is
  known) while moving the flag-string assembly into the shared helper.

### Effect on a search task

Each search task's command block shrinks from ~80 lines to roughly:

```bash
set -euo pipefail
source /usr/local/bin/sn_resource_monitor.sh
source /usr/local/bin/sn_download.sh
source /usr/local/bin/sn_import_flags.sh

sn_monitor_start
sn_download_inputs "${input_dir}" ~{write_lines(input_files)}
import_flags="$(sn_build_import_flags)"

spectronaut ${import_flags} direct -d "${input_dir}" -fasta ... -o ... -setTemp ...

sn_monitor_report
```

The per-task `runtime {}` blocks, resource presets, and Spectronaut command lines are
**unchanged** — only the shared shell scaffolding moves into the image.

---

## S1 — Single multi-source download (inside `sn_download.sh`)

### Problem (corrected understanding)

`gcloud storage cp` already parallelizes in two dimensions: across multiple source
objects passed to one invocation, and within a single large object (sliced/composite
transfer). The current code defeats the **cross-object** dimension by passing exactly one
source per call inside a shell loop:

```bash
while IFS= read -r input_file; do
    gcloud storage cp -r "${input_file}" "${input_dir}/"   # one source per call
done < ~{write_lines(input_files)}
```

Consequence by input type:

- **`.htrms` (single objects):** each `cp` moves one file; cross-object parallelism cannot
  engage. N files → N sequential invocations, each paying gcloud's ~1–2 s startup. Genuinely serial.
- **`.d` directories (timsTOF):** `-r` parallelizes across the many objects *inside* one
  `.d`, so each directory transfers fast — but the outer loop still processes one `.d` at
  a time, serially across directories.

So the fix is not "make `cp` parallel" (it already is internally) but **collapse the loop
into one multi-source `cp`** so gcloud fans out across the entire bin at once and pays
startup cost once.

### Implementation

```bash
sn_download_inputs() {   # $1 = dest_dir, $2 = paths_file
    local dest="$1" paths="$2"

    # Single invocation. gcloud parallelizes across all sources (and across all objects
    # within any .d directories), paying process startup once.
    # --read-paths-from-stdin avoids ARG_MAX limits for large bins with long GCS URIs.
    gcloud storage cp -r --read-paths-from-stdin "${dest}/" < "${paths}"

    # Loud verification: fail fast and clearly on a partial/failed transfer.
    local expected actual
    expected=$(grep -c . "${paths}")
    actual=$(find "${dest}" -mindepth 1 -maxdepth 1 | wc -l)
    if [ "${actual}" -lt "${expected}" ]; then
        echo "ERROR: expected ${expected} downloaded item(s), found ${actual}" >&2
        exit 1
    fi
}
```

### Caveats folded into the design

- **ARG_MAX:** a bin near the file cap with long URIs could overflow a command line.
  `--read-paths-from-stdin` (stdin) is the primary form to avoid this. If the installed
  gcloud in the `v21.0` image does not support that flag, fall back to `xargs`-based
  batching (verified during implementation).
- **Error granularity:** one bulk `cp` fails as a unit rather than per file. The
  post-download count assertion above restores loud, early failure so a missing input is
  caught at download time, not cryptically inside Spectronaut later.
- **Directory-heavy bins:** for `.d`-only bins the marginal win is smaller (internals were
  already parallel), but per-directory startup is still saved and cross-directory
  transfers now overlap.

---

## Testing & rollout

Ordered so the image exists before the WDL that depends on it is exercised:

1. **Build + push image.** Rebuild `broadcptacdev/panoply_spectronaut:v21.0` with the three
   helper scripts. Smoke-test that `source /usr/local/bin/sn_resource_monitor.sh` (and the
   other two) resolve and their functions are callable inside the container.
2. **Validate WDL.** `womtool validate` and `miniwdl check` on the edited workflow — both
   must stay clean with no new warnings — then `dos2unix`.
3. **Byte-identical behavior gate.** On a small sample run, capture each affected task's
   stdout before and after. Diff the resource-monitor report block and the download-summary
   lines; they must match except for timing and paths.
4. **End-to-end.** One full parallel run (`num_vms > 1`, `do_pulsar = true`) confirming the
   final `spectronaut_output.zip` matches a pre-change baseline.

**Coupled release:** the WDL edit and the `v21.0` image rebuild land together.

---

## Risks

| Risk | Mitigation |
|---|---|
| WDL sources helpers missing from a stale image | Coupled release; image built + smoke-tested before WDL run (step 1 before 3). |
| Extracted helper changes resource-report text | Byte-identical diff gate (test step 3). |
| Bulk `cp` hides a partial-download failure | Post-download count assertion in `sn_download_inputs`. |
| `--read-paths-from-stdin` unsupported by image's gcloud | Verify during implementation; `xargs` fallback specified. |
| Refactor drifts a Spectronaut command line | Command lines are copied verbatim into thinned tasks; e2e output diff (test step 4). |

---

## Out of scope / future follow-up

- Apply the same helper extraction to `v19.7` / `v20.3` / `v20.4` / `v20.5` images and the
  `regular_directDIA` + `sne_combine` workflows for consistency.
- Revisit R2 (machine-type guard) if scheduling/stockout failures appear, once the backend
  machine family is confirmed.
- Sync `CLAUDE.md` after implementation (deferred; owner will update manually).

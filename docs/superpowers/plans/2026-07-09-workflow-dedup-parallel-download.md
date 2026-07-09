# Spectronaut Workflow Dedup + Parallel Download Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Extract duplicated shell boilerplate from the six Spectronaut-image tasks into image-baked helper scripts, and replace the serial per-file GCS download loop with a single multi-source parallel download.

**Architecture:** Three shell helper scripts (`sn_resource_monitor.sh`, `sn_download.sh`, `sn_import_flags.sh`) are baked into the `v21.0` Docker image at `/usr/local/bin/` and `source`d by each Spectronaut-image task. The tasks' command blocks shrink to: source helpers → `sn_monitor_start` → `sn_download_inputs` → build search-specific flags → run `spectronaut` → `sn_monitor_report`. The download helper collapses the old per-file `while … gcloud storage cp` loop into one `gcloud storage cp -r --read-paths-from-stdin` call.

**Tech Stack:** WDL (`version development`, Cromwell/Terra), Bash, Docker (`ubuntu:22.04` base), gcloud CLI, Spectronaut. Validators: `womtool`, `miniwdl`. Line-ending tool: `dos2unix`.

## Global Constraints

- **WDL version:** `version development` (required for `Directory` type) — do not change.
- **Image scope:** Only rebuild `broadcptacdev/panoply_spectronaut:v21.0`. Do NOT touch `v19.7`/`v20.3`/`v20.4`/`v20.5` dockerfiles.
- **Task scope:** Only the six Spectronaut-image tasks may source helpers: `htrms_conversion`, `directDIA_single_vm`, `pulsar_step1_binned`, `pulsar_step3_binned`, `dia_analysis_binned`, `combine_sne`. Leave `list_files`, `create_bins`, `sum_floats`, `validate_skip_pulsar` untouched.
- **Behavior preservation:** The resource-usage report text/format and the download semantics must be byte-identical to today (except timing/paths). Spectronaut command lines copied verbatim.
- **Coupled release:** The WDL change and the `v21.0` image rebuild ship together — the WDL sourcing helpers fails on an image lacking them.
- **Build context is the repo root.** Dockerfiles `COPY` from repo-root-relative paths; build with `-f src/docker/... .`.
- **After editing any `.wdl`:** run `dos2unix <file.wdl>`.
- **Helper install path:** `/usr/local/bin/sn_resource_monitor.sh`, `/usr/local/bin/sn_download.sh`, `/usr/local/bin/sn_import_flags.sh` (executable).
- **Helper source location in repo:** `src/docker/helpers/` (COPYed into the image).
- **Commit style:** Conventional Commits (`feat:`, `refactor:`, `chore:`, `docs:`, `test:`).

---

### Task 1: Create `sn_resource_monitor.sh` helper

Extracts the cgroup v1/v2 resource-monitoring boilerplate (currently duplicated across ~8 tasks) into two sourced functions producing byte-identical report output.

**Files:**
- Create: `src/docker/helpers/sn_resource_monitor.sh`
- Test: `src/docker/helpers/tests/test_sn_resource_monitor.sh`

**Interfaces:**
- Produces:
  - `sn_monitor_start` — sets globals `wall_start`, `cpu_start_ns`, `cpu_stat_start`. No args.
  - `sn_monitor_report <allocated_cpus> <allocated_disk_gb> <root_dir>` — prints the RESOURCE USAGE REPORT block using the globals from `sn_monitor_start`.

- [ ] **Step 1: Write the helper script**

Create `src/docker/helpers/sn_resource_monitor.sh`. The two functions contain the exact logic currently at `parallel_spectronaut.wdl:1011-1026` (start) and `:1109-1224` (report), with `~{cpu}`/`~{allocated_disk_gb}`/`${cromwell_root}` replaced by function arguments `$1`/`$2`/`$3`:

```bash
#!/usr/bin/env bash
# Shared Spectronaut task resource monitor. Source, then call sn_monitor_start at the
# top of a command block and sn_monitor_report at the end. Output is intentionally
# byte-identical to the previously-inlined report so existing log parsing is unaffected.

sn_monitor_start() {
    wall_start=$(date +%s%N)  # Nanoseconds since epoch

    # Cgroup V2 (modern)
    if [ -f /sys/fs/cgroup/cpu.stat ]; then
        cpu_start=$(grep "usage_usec" /sys/fs/cgroup/cpu.stat | awk '{print $2}')
        cpu_start_ns=$((cpu_start * 1000))  # Convert microseconds to nanoseconds
    # Cgroup V1 (legacy)
    elif [ -f /sys/fs/cgroup/cpuacct/cpuacct.usage ]; then
        cpu_start_ns=$(cat /sys/fs/cgroup/cpuacct/cpuacct.usage)
    else
        cpu_start_ns=0
    fi

    # Capture per-CPU stats from /proc/stat
    cpu_stat_start=$(grep "^cpu[0-9]" /proc/stat 2>/dev/null || true)
}

sn_monitor_report() {
    local allocated_cpus="$1"
    local allocated_disk_gb="$2"
    local root_dir="$3"

    echo "==========================================="
    echo "=== RESOURCE USAGE REPORT ==="
    echo "==========================================="

    # --- CPU Utilization ---
    echo ""
    echo "--- CPU Utilization ---"

    wall_end=$(date +%s%N)
    wall_elapsed_ns=$((wall_end - wall_start))

    # Cgroup V2
    if [ -f /sys/fs/cgroup/cpu.stat ]; then
        cpu_end=$(grep "usage_usec" /sys/fs/cgroup/cpu.stat | awk '{print $2}')
        cpu_end_ns=$((cpu_end * 1000))
    # Cgroup V1
    elif [ -f /sys/fs/cgroup/cpuacct/cpuacct.usage ]; then
        cpu_end_ns=$(cat /sys/fs/cgroup/cpuacct/cpuacct.usage)
    else
        cpu_end_ns=0
    fi

    cpu_stat_end=$(grep "^cpu[0-9]" /proc/stat 2>/dev/null || true)

    if [ "$cpu_start_ns" -gt 0 ] && [ "$cpu_end_ns" -gt "$cpu_start_ns" ] && [ "$wall_elapsed_ns" -gt 0 ]; then
        cpu_used_ns=$((cpu_end_ns - cpu_start_ns))
        cpu_utilization_total=$(awk -v cpu="$cpu_used_ns" -v wall="$wall_elapsed_ns" \
            'BEGIN { printf "%.2f", (cpu / wall) * 100 }')
        cpu_utilization_per_core=$(awk -v total="$cpu_utilization_total" -v cpus="$allocated_cpus" \
            'BEGIN { printf "%.2f", total / cpus }')

        echo "Allocated CPUs: ${allocated_cpus}"
        echo "Average CPU Utilization: ${cpu_utilization_total}% (across all cores)"

        if [ -n "$cpu_stat_start" ] && [ -n "$cpu_stat_end" ]; then
            max_core_util=$(paste \
                <(echo "$cpu_stat_start" | awk '{idle=$5; total=0; for(i=2;i<=NF;i++) total+=$i; print idle, total}') \
                <(echo "$cpu_stat_end"   | awk '{idle=$5; total=0; for(i=2;i<=NF;i++) total+=$i; print idle, total}') \
                | awk '{
                    idle_start=$1; total_start=$2; idle_end=$3; total_end=$4;
                    d_total = total_end - total_start;
                    d_idle  = idle_end  - idle_start;
                    if (d_total > 0) util = (1 - d_idle/d_total) * 100; else util = 0;
                    if (util > max) max = util;
                  } END { printf "%.2f", max }')
            echo "Max Per-Core CPU Utilization: ${max_core_util}%"
        fi

        echo "Per-Core CPU Utilization: ${cpu_utilization_per_core}%"
        echo "Maximum Possible: $((allocated_cpus * 100))% (${allocated_cpus} cores at 100%)"
    else
        echo "CPU utilization data not available"
    fi

    # --- Memory Usage ---
    echo ""
    echo "--- Memory Usage ---"

    # Cgroup V2
    if [ -f /sys/fs/cgroup/memory.peak ]; then
        max_mem_bytes=$(cat /sys/fs/cgroup/memory.peak)
        limit_bytes=$(cat /sys/fs/cgroup/memory.max)
    # Cgroup V1
    elif [ -f /sys/fs/cgroup/memory/memory.max_usage_in_bytes ]; then
        max_mem_bytes=$(cat /sys/fs/cgroup/memory/memory.max_usage_in_bytes)
        limit_bytes=$(cat /sys/fs/cgroup/memory/memory.limit_in_bytes)
    else
        max_mem_bytes=0
        limit_bytes=0
    fi

    if [ "$max_mem_bytes" -gt 0 ]; then
        max_mem_gb=$(awk -v val="$max_mem_bytes" 'BEGIN { printf "%.2f", val / (1024^3) }')
        echo "Actual Peak RAM: ${max_mem_gb} GB"

        if [ "$limit_bytes" != "max" ] && [ "$limit_bytes" -gt 0 ]; then
            limit_gb=$(awk -v val="$limit_bytes" 'BEGIN { printf "%.2f", val / (1024^3) }')
            usage_percent=$(awk -v max="$max_mem_bytes" -v lim="$limit_bytes" \
                'BEGIN { printf "%.2f", (max / lim) * 100 }')
            echo "RAM Limit: ${limit_gb} GB"
            echo "RAM Usage: ${usage_percent}%"
        fi
    else
        echo "Memory usage data not available"
    fi

    # --- Disk Usage ---
    echo ""
    echo "--- Disk Usage ---"

    echo "Disk Size Assigned: ${allocated_disk_gb} GB"

    if command -v df >/dev/null 2>&1; then
        disk_used_output=$(df -BG "${root_dir}" 2>/dev/null | tail -1 | awk '{print $3}')

        if [ -n "${disk_used_output}" ]; then
            disk_used_gb=$(echo "${disk_used_output}" | sed 's/G$//')
            disk_usage_percent=$(awk -v used="$disk_used_gb" -v alloc="$allocated_disk_gb" \
                'BEGIN { printf "%.2f", (used / alloc) * 100 }')

            echo "Final Disk Used: ${disk_used_gb} GB"
            echo "Disk Usage: ${disk_usage_percent}%"
        else
            echo "Could not measure disk usage"
        fi
    else
        echo "df command not available"
    fi

    echo ""
    echo "==========================================="
}
```

- [ ] **Step 2: Write the test script**

Create `src/docker/helpers/tests/test_sn_resource_monitor.sh`. It sources the helper and asserts the report renders the expected section headers and handles the no-cgroup fallback path (the report must not crash when cgroup files are absent, e.g. on a dev laptop):

```bash
#!/usr/bin/env bash
set -euo pipefail
HERE="$(cd "$(dirname "$0")/.." && pwd)"
source "${HERE}/sn_resource_monitor.sh"

sn_monitor_start
out="$(sn_monitor_report 64 300 "$(pwd)")"

echo "$out"

# Report must contain the fixed section markers regardless of cgroup availability.
for marker in "=== RESOURCE USAGE REPORT ===" "--- CPU Utilization ---" "--- Memory Usage ---" "--- Disk Usage ---" "Disk Size Assigned: 300 GB"; do
    if ! grep -qF "$marker" <<<"$out"; then
        echo "FAIL: missing marker: $marker" >&2
        exit 1
    fi
done
echo "PASS: sn_resource_monitor renders all report sections"
```

- [ ] **Step 3: Run the test to verify it passes**

Run: `bash src/docker/helpers/tests/test_sn_resource_monitor.sh`
Expected: prints the report, then `PASS: sn_resource_monitor renders all report sections`. (On a machine without cgroup files, CPU/memory lines say "not available" — that is expected and the markers still render.)

- [ ] **Step 4: Commit**

```bash
chmod +x src/docker/helpers/sn_resource_monitor.sh src/docker/helpers/tests/test_sn_resource_monitor.sh
git add src/docker/helpers/sn_resource_monitor.sh src/docker/helpers/tests/test_sn_resource_monitor.sh
git commit -m "feat: add sn_resource_monitor.sh shared task helper"
```

---

### Task 2: Create `sn_import_flags.sh` helper

Replaces the ~21 inline `~{if defined(enzyme_database) ...}` / `--importModRepository` repetitions with one function that assembles the flag prefix from env vars.

**Files:**
- Create: `src/docker/helpers/sn_import_flags.sh`
- Test: `src/docker/helpers/tests/test_sn_import_flags.sh`

**Interfaces:**
- Produces:
  - `sn_build_import_flags` — reads env vars `ENZYME_DB` and `MOD_REPO`; echoes `--importEnzymeDB <ENZYME_DB> --importModRepository <MOD_REPO>`, omitting each flag whose var is empty; echoes empty string when both empty.

- [ ] **Step 1: Write the helper script**

Create `src/docker/helpers/sn_import_flags.sh`:

```bash
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
```

- [ ] **Step 2: Write the test script**

Create `src/docker/helpers/tests/test_sn_import_flags.sh`:

```bash
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
```

- [ ] **Step 3: Run the test to verify it passes**

Run: `bash src/docker/helpers/tests/test_sn_import_flags.sh`
Expected: `PASS: sn_import_flags handles all four gating cases`

- [ ] **Step 4: Commit**

```bash
chmod +x src/docker/helpers/sn_import_flags.sh src/docker/helpers/tests/test_sn_import_flags.sh
git add src/docker/helpers/sn_import_flags.sh src/docker/helpers/tests/test_sn_import_flags.sh
git commit -m "feat: add sn_import_flags.sh shared task helper"
```

---

### Task 3: Create `sn_download.sh` helper (with parallel download — S1)

Owns GCS input/library download. Replaces the serial per-file `gcloud storage cp` loop with one multi-source call plus a loud post-download count assertion.

**Files:**
- Create: `src/docker/helpers/sn_download.sh`
- Test: `src/docker/helpers/tests/test_sn_download.sh`

**Interfaces:**
- Consumes: a `gcloud` binary on PATH (present in the `v21.0` image).
- Produces:
  - `sn_download_inputs <dest_dir> <paths_file>` — downloads every GCS path listed in `paths_file` into `dest_dir` in one `gcloud storage cp` call; exits non-zero if fewer top-level items land than paths requested.
  - `sn_download_libraries <dest_dir> <paths_file>` — downloads each library path and echoes the space-joined `-a <local_path>` argument string.

- [ ] **Step 1: Write the helper script**

Create `src/docker/helpers/sn_download.sh`. `sn_download_inputs` uses `--read-paths-from-stdin` (ARG_MAX-safe) with an `xargs` fallback if the flag is unsupported:

```bash
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
        # Fallback for older gcloud: xargs batches sources across a few cp calls.
        grep -v '^[[:space:]]*$' "${paths}" | xargs -r -d '\n' -I{} echo {} \
            | gcloud storage cp -r -I "${dest}/" 2>/dev/null \
            || xargs -r -d '\n' gcloud storage cp -r "${dest}/" < "${paths}"
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
            gcloud storage cp -r "${lib_path}" "${dest}/" >&2
            lib_args="${lib_args} -a ${dest}/$(basename "${lib_path}")"
        fi
    done < "${paths}"
    echo "${lib_args# }"
}
```

- [ ] **Step 2: Write the test script**

Create `src/docker/helpers/tests/test_sn_download.sh`. It stubs `gcloud` with a fake on PATH so the test runs without cloud access, and verifies (a) a single multi-source invocation is used and (b) the count assertion fails loudly on a short download:

```bash
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

echo "PASS: sn_download parallel download + loud count assertion"
```

- [ ] **Step 3: Run the test to verify it passes**

Run: `bash src/docker/helpers/tests/test_sn_download.sh`
Expected: `PASS: sn_download parallel download + loud count assertion`

- [ ] **Step 4: Commit**

```bash
chmod +x src/docker/helpers/sn_download.sh src/docker/helpers/tests/test_sn_download.sh
git add src/docker/helpers/sn_download.sh src/docker/helpers/tests/test_sn_download.sh
git commit -m "feat: add sn_download.sh helper with single-call parallel download"
```

---

### Task 4: Bake helpers into the v21.0 Docker image

Adds the three helper scripts to the image at `/usr/local/bin/` and smoke-tests they source and run inside the container.

**Files:**
- Modify: `src/docker/spectronaut_v21.0.dockerfile` (add a `COPY` + `chmod` block after the Spectronaut install `RUN`, before line 57's `activate`)

**Interfaces:**
- Consumes: `src/docker/helpers/*.sh` from Tasks 1–3.
- Produces: image `broadcptacdev/panoply_spectronaut:v21.0` with `/usr/local/bin/sn_resource_monitor.sh`, `/usr/local/bin/sn_download.sh`, `/usr/local/bin/sn_import_flags.sh` (executable).

- [ ] **Step 1: Add the COPY + chmod block to the Dockerfile**

Insert after the Spectronaut `.deb` install `RUN` (currently ending at line 55) and before `RUN spectronaut activate ...` (line 57):

```dockerfile
# Copy shared task helper scripts (sourced by WDL task command blocks)
COPY src/docker/helpers/sn_resource_monitor.sh /usr/local/bin/sn_resource_monitor.sh
COPY src/docker/helpers/sn_download.sh          /usr/local/bin/sn_download.sh
COPY src/docker/helpers/sn_import_flags.sh      /usr/local/bin/sn_import_flags.sh
RUN chmod +x /usr/local/bin/sn_resource_monitor.sh \
             /usr/local/bin/sn_download.sh \
             /usr/local/bin/sn_import_flags.sh
```

- [ ] **Step 2: Build the image (linux/amd64)**

Run from the repo root (build context must be repo root — the `COPY` paths are repo-relative):

```bash
docker buildx build \
  --platform linux/amd64 \
  -t broadcptacdev/panoply_spectronaut:v21.0 \
  -f src/docker/spectronaut_v21.0.dockerfile \
  --load .
```

Expected: build succeeds; the `COPY` and `chmod` layers complete without error. (Use `--load` for local smoke-test; `--push` happens in Step 4.)

- [ ] **Step 3: Smoke-test that helpers source and functions are callable in the container**

```bash
docker run --rm --platform linux/amd64 broadcptacdev/panoply_spectronaut:v21.0 bash -c '
  set -euo pipefail
  source /usr/local/bin/sn_resource_monitor.sh
  source /usr/local/bin/sn_download.sh
  source /usr/local/bin/sn_import_flags.sh
  type sn_monitor_start sn_monitor_report sn_download_inputs sn_download_libraries sn_build_import_flags >/dev/null
  ENZYME_DB="" MOD_REPO="" ; export ENZYME_DB MOD_REPO
  test -z "$(sn_build_import_flags)"
  echo "SMOKE-OK: all five helper functions callable in image"
'
```

Expected: `SMOKE-OK: all five helper functions callable in image`

- [ ] **Step 4: Push the image**

```bash
docker buildx build \
  --platform linux/amd64 \
  -t broadcptacdev/panoply_spectronaut:v21.0 \
  -f src/docker/spectronaut_v21.0.dockerfile \
  --push .
```

Expected: push completes; the `v21.0` tag now includes the helpers. **This must happen before Tasks 5–10's WDL is run in production (coupled release).**

- [ ] **Step 5: Commit**

```bash
git add src/docker/spectronaut_v21.0.dockerfile
git commit -m "chore: bake sn_* task helpers into v21.0 image"
```

---

### Task 5: Refactor `pulsar_step1_binned` to use helpers

Thins the first search task's command block: source helpers, use `sn_monitor_start`/`sn_download_inputs`/`sn_build_import_flags`/`sn_monitor_report`. This is the pattern the remaining tasks follow.

**Files:**
- Modify: `wdl_workflow/parallelized_search/parallel_spectronaut.wdl` — task `pulsar_step1_binned` (command block ~`:1006-1225`)

**Interfaces:**
- Consumes: `sn_monitor_start`, `sn_monitor_report <cpu> <disk> <root>`, `sn_download_inputs <dest> <paths_file>`, `sn_build_import_flags` (env `ENZYME_DB`/`MOD_REPO`) from Tasks 1–3, present in the image from Task 4.
- Produces: unchanged task outputs (`intermediate_archive = glob("*.psar")[0]`).

- [ ] **Step 1: Replace the command block**

In `pulsar_step1_binned`'s `command <<< … >>>`, replace the inline resource-monitor init (`:1011-1026`), the download loop (`:1037-1049`), the inline import-flag interpolations (`:1084-1085` and inside `cmd_string`), and the inline resource-report (`:1109-1224`) with helper calls. The `.htrms`-vs-`.d` flag detection and the `spectronaut` invocation stay. Resulting command block:

```bash
        set -euo pipefail

        cromwell_root=$(pwd)
        source /usr/local/bin/sn_resource_monitor.sh
        source /usr/local/bin/sn_download.sh
        source /usr/local/bin/sn_import_flags.sh

        sn_monitor_start

        input_dir="${cromwell_root}/work_input"
        output_dir="${cromwell_root}/out_pulsar_step1"
        tmp_dir="${cromwell_root}/sn_temp"
        mkdir -p "${output_dir}" "${tmp_dir}"

        sn_download_inputs "${input_dir}" ~{write_lines(input_files)}

        echo "Files in input directory:"
        ls -1 "${input_dir}"

        # HTRMS files require -r per file; raw files (incl. timsTOF .d dirs) use -d directory.
        htrms_count=$(find "${input_dir}" -mindepth 1 -maxdepth 1 -name "*.htrms" | wc -l)
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

        export ENZYME_DB="~{default='' enzyme_database}"
        export MOD_REPO="~{default='' custom_mod_repository}"
        import_flags="$(sn_build_import_flags)"

        spectronaut \
            -setTemp "${tmp_dir}" \
            ${import_flags} \
            lg -se Pulsar \
            ${cmd_flags} \
            -fasta "~{fasta_1}" \
            ~{if defined(fasta_2) then "-fasta " + fasta_2 else ""} \
            ~{if defined(fasta_3) then "-fasta " + fasta_3 else ""} \
            -rs "~{analysis_schema}" \
            --pulsarStage pulsarStep1 \
            -a "${output_dir}/search_archive_step1_bin_~{bin_index}.psar" \
            -o "${output_dir}" \
            2>&1 | tee pulsar_step1_bin_~{bin_index}.log

        psar_count=$(find "${output_dir}" -type f -name "*.psar" | wc -l)
        if [ "${psar_count}" -eq 0 ]; then
            echo "ERROR: No .psar file produced for bin ~{bin_index}" >&2
            echo "Output directory contents:" >&2
            ls -lh "${output_dir}" >&2
            exit 1
        fi
        echo "Moving ${psar_count} PSAR file(s)..."
        find "${output_dir}" -type f -name "*.psar" -exec mv {} "${cromwell_root}/" \;
        echo "Generated intermediate search archive for bin ~{bin_index}"

        sn_monitor_report ~{cpu} ~{allocated_disk_gb} "${cromwell_root}"
```

- [ ] **Step 2: Convert line endings**

Run: `dos2unix wdl_workflow/parallelized_search/parallel_spectronaut.wdl`
Expected: `converting file … to Unix format`

- [ ] **Step 3: Validate the WDL**

Run:
```bash
womtool validate wdl_workflow/parallelized_search/parallel_spectronaut.wdl
miniwdl check wdl_workflow/parallelized_search/parallel_spectronaut.wdl
```
Expected: `womtool` prints `Success!`; `miniwdl` prints no NEW errors/warnings beyond the pre-existing ones (`UnusedCall` on `validate_skip_pulsar`, `FileCoercion`/`StringCoercion`, `SC2001`). If a new warning appears that references the edited task, fix it before committing.

- [ ] **Step 4: Commit**

```bash
git add wdl_workflow/parallelized_search/parallel_spectronaut.wdl
git commit -m "refactor: thin pulsar_step1_binned via sn_* helpers"
```

---

### Task 6: Refactor `pulsar_step3_binned` to use helpers

Same pattern as Task 5, applied to the final-archive search task. Preserves its distinct `spectronaut` invocation (uses `--pulsarStage pulsarStep3`, the optimized models, and the step-1 archive).

**Files:**
- Modify: `wdl_workflow/parallelized_search/parallel_spectronaut.wdl` — task `pulsar_step3_binned` (command block ~`:1476-1700`)

**Interfaces:**
- Consumes: same helper functions as Task 5.
- Produces: unchanged task outputs (`final_archive`).

- [ ] **Step 1: Read the current task to capture its exact spectronaut invocation**

Run: `sed -n '1455,1710p' wdl_workflow/parallelized_search/parallel_spectronaut.wdl`
Expected: shows `pulsar_step3_binned`. Copy its `spectronaut …` command and its output-verification block **verbatim** into Step 2 — do not paraphrase flags.

- [ ] **Step 2: Replace the command block**

Apply the same head/tail helper substitution as Task 5 (source helpers → `sn_monitor_start` → dir setup → `sn_download_inputs` → `.htrms`/`.d` detection → `export ENZYME_DB/MOD_REPO` + `import_flags="$(sn_build_import_flags)"` → verbatim `spectronaut` invocation with `${import_flags}` in place of the two inline `~{if defined(enzyme_database)…}`/`~{if defined(custom_mod_repository)…}` fragments → verbatim output verification/move → `sn_monitor_report ~{cpu} ~{allocated_disk_gb} "${cromwell_root}"`). Keep step-3-specific inputs (`intermediate_archive`, `optimized_models`) and directory names (`out_pulsar_step3`) exactly as they are today.

- [ ] **Step 3: Convert line endings**

Run: `dos2unix wdl_workflow/parallelized_search/parallel_spectronaut.wdl`

- [ ] **Step 4: Validate the WDL**

Run:
```bash
womtool validate wdl_workflow/parallelized_search/parallel_spectronaut.wdl
miniwdl check wdl_workflow/parallelized_search/parallel_spectronaut.wdl
```
Expected: `Success!` and no new warnings for `pulsar_step3_binned`.

- [ ] **Step 5: Commit**

```bash
git add wdl_workflow/parallelized_search/parallel_spectronaut.wdl
git commit -m "refactor: thin pulsar_step3_binned via sn_* helpers"
```

---

### Task 7: Refactor `dia_analysis_binned` to use helpers

Same pattern. This task also downloads user spectral libraries, so it uses `sn_download_libraries` in addition to `sn_download_inputs`.

**Files:**
- Modify: `wdl_workflow/parallelized_search/parallel_spectronaut.wdl` — task `dia_analysis_binned` (command block ~`:1931-2150`)

**Interfaces:**
- Consumes: helper functions from Tasks 1–3, plus `sn_download_libraries <dest> <paths_file>` returning the `-a <lib>` arg string.
- Produces: unchanged task outputs (`sne_files`).

- [ ] **Step 1: Read the current task to capture its exact spectronaut invocation and library-download logic**

Run: `sed -n '1909,2155p' wdl_workflow/parallelized_search/parallel_spectronaut.wdl`
Expected: shows `dia_analysis_binned`, including the user-library download loop (`:1980-1990` region) and the `diaanalysis` invocation. Copy the `spectronaut diaanalysis …` command verbatim into Step 2.

- [ ] **Step 2: Replace the command block**

Apply the head/tail helper substitution (as Task 5), and replace the inline user-library download loop with:

```bash
        user_lib_dir="${cromwell_root}/user_libraries"
        user_lib_args="$(sn_download_libraries "${user_lib_dir}" ~{write_lines(user_spectral_libraries)})"
```

Keep the existing `merged_archive` handling (the Pulsar `.kit`) and the `diaanalysis` invocation verbatim, substituting `${import_flags}` for the inline import fragments and `${user_lib_args}` where the per-lib `-a` flags were assembled. Preserve dia-analysis-specific inputs and the `-n ~{experiment_name}` / `-o` / `-setTemp` flags exactly.

- [ ] **Step 3: Convert line endings**

Run: `dos2unix wdl_workflow/parallelized_search/parallel_spectronaut.wdl`

- [ ] **Step 4: Validate the WDL**

Run:
```bash
womtool validate wdl_workflow/parallelized_search/parallel_spectronaut.wdl
miniwdl check wdl_workflow/parallelized_search/parallel_spectronaut.wdl
```
Expected: `Success!` and no new warnings for `dia_analysis_binned`.

- [ ] **Step 5: Commit**

```bash
git add wdl_workflow/parallelized_search/parallel_spectronaut.wdl
git commit -m "refactor: thin dia_analysis_binned via sn_* helpers"
```

---

### Task 8: Refactor `directDIA_single_vm` to use helpers

Same pattern. Single-VM search that also downloads user libraries. Preserves its `-direct`/`diaanalysis` branching and single-VM semantics.

**Files:**
- Modify: `wdl_workflow/parallelized_search/parallel_spectronaut.wdl` — task `directDIA_single_vm` (command block ~`:726-975`)

**Interfaces:**
- Consumes: helper functions from Tasks 1–3, including `sn_download_libraries`.
- Produces: unchanged task outputs (`spectronaut_output`).

- [ ] **Step 1: Read the current task to capture its exact invocation**

Run: `sed -n '695,980p' wdl_workflow/parallelized_search/parallel_spectronaut.wdl`
Expected: shows `directDIA_single_vm`, including its input/library download loops and the `spectronaut` invocation(s). Copy verbatim into Step 2.

- [ ] **Step 2: Replace the command block**

Apply the head/tail helper substitution, swap the input-download loop for `sn_download_inputs`, swap the library loop for `sn_download_libraries`, set `export ENZYME_DB/MOD_REPO` + `import_flags`, and keep the single-VM `spectronaut` invocation and all its flags verbatim. Preserve directory names (`work_input`, output dirs) and the `skip_pulsar`/`user_spectral_libraries` handling exactly.

- [ ] **Step 3: Convert line endings**

Run: `dos2unix wdl_workflow/parallelized_search/parallel_spectronaut.wdl`

- [ ] **Step 4: Validate the WDL**

Run:
```bash
womtool validate wdl_workflow/parallelized_search/parallel_spectronaut.wdl
miniwdl check wdl_workflow/parallelized_search/parallel_spectronaut.wdl
```
Expected: `Success!` and no new warnings for `directDIA_single_vm`.

- [ ] **Step 5: Commit**

```bash
git add wdl_workflow/parallelized_search/parallel_spectronaut.wdl
git commit -m "refactor: thin directDIA_single_vm via sn_* helpers"
```

---

### Task 9: Refactor `htrms_conversion` and `combine_sne` monitor/import blocks

These two Spectronaut-image tasks use the resource monitor and (for `combine_sne`) import flags, but have task-specific download patterns. Extract only the monitor and import-flag boilerplate; leave their download logic as-is where it differs.

**Files:**
- Modify: `wdl_workflow/parallelized_search/parallel_spectronaut.wdl` — tasks `htrms_conversion` (~`:645-690`) and `combine_sne` (~`:2175-2385`)

**Interfaces:**
- Consumes: `sn_monitor_start`, `sn_monitor_report`, `sn_build_import_flags` from Tasks 1–2.
- Produces: unchanged task outputs.

- [ ] **Step 1: Read both tasks to capture their exact structure**

Run:
```bash
sed -n '633,695p'  wdl_workflow/parallelized_search/parallel_spectronaut.wdl   # htrms_conversion
sed -n '2160,2390p' wdl_workflow/parallelized_search/parallel_spectronaut.wdl  # combine_sne
```
Expected: shows both tasks. Note `combine_sne` copies SNE files from a Cromwell-localized `Array[File]` (not a GCS download loop) — leave that copy logic intact.

- [ ] **Step 2: Apply monitor + import-flag substitution**

In each task: add `source /usr/local/bin/sn_resource_monitor.sh` (and `sn_import_flags.sh` for `combine_sne`) after `cromwell_root=$(pwd)`; replace the inline monitor-init with `sn_monitor_start`; replace the inline resource-report tail with `sn_monitor_report ~{cpu} ~{allocated_disk_gb} "${cromwell_root}"`; for `combine_sne`, set `export ENZYME_DB/MOD_REPO` + `import_flags="$(sn_build_import_flags)"` and substitute `${import_flags}` for the inline import fragments. Do **not** alter `combine_sne`'s SNE-file copy loop or `htrms_conversion`'s conversion command.

- [ ] **Step 3: Convert line endings**

Run: `dos2unix wdl_workflow/parallelized_search/parallel_spectronaut.wdl`

- [ ] **Step 4: Validate the WDL**

Run:
```bash
womtool validate wdl_workflow/parallelized_search/parallel_spectronaut.wdl
miniwdl check wdl_workflow/parallelized_search/parallel_spectronaut.wdl
```
Expected: `Success!` and no new warnings for `htrms_conversion` or `combine_sne`.

- [ ] **Step 5: Commit**

```bash
git add wdl_workflow/parallelized_search/parallel_spectronaut.wdl
git commit -m "refactor: thin htrms_conversion and combine_sne via sn_* helpers"
```

---

### Task 10: End-to-end behavior-parity verification

Confirms the refactor preserved behavior: WDL still valid, resource-report format unchanged, and (if cloud access available) a full parallel run matches a pre-change baseline.

**Files:**
- Test: `docs/superpowers/plans/2026-07-09-workflow-dedup-parallel-download.md` (this plan — checklist only; no code file)

**Interfaces:**
- Consumes: the fully refactored WDL (Tasks 5–9) and pushed image (Task 4).

- [ ] **Step 1: Final full-file validation**

Run:
```bash
womtool validate wdl_workflow/parallelized_search/parallel_spectronaut.wdl
miniwdl check wdl_workflow/parallelized_search/parallel_spectronaut.wdl
grep -c "RESOURCE USAGE REPORT" wdl_workflow/parallelized_search/parallel_spectronaut.wdl
grep -c "usage_usec" wdl_workflow/parallelized_search/parallel_spectronaut.wdl
```
Expected: `Success!`; the two `grep -c` counts drop to **0** (the inline report boilerplate is gone from the WDL — now only in the image), confirming the extraction actually removed duplication rather than leaving it in place.

- [ ] **Step 2: Confirm helper unit tests still pass**

Run:
```bash
bash src/docker/helpers/tests/test_sn_resource_monitor.sh
bash src/docker/helpers/tests/test_sn_import_flags.sh
bash src/docker/helpers/tests/test_sn_download.sh
```
Expected: three `PASS:` lines.

- [ ] **Step 3: (Cloud-gated) Sample-run stdout parity**

If a Terra/Cromwell test environment is available, run one small parallel job (`num_vms > 1`, `do_pulsar = true`) against the rebuilt `v21.0` image. Capture one search task's stdout and confirm the RESOURCE USAGE REPORT block renders the same section headers and that "Downloaded N input item(s)" appears once (single-call download) rather than N "Downloading:" lines.

Expected: report sections identical to a pre-change run; download log shows the single-call form. If no cloud environment is available, record this step as **deferred** and note it in the PR description — do not silently skip.

- [ ] **Step 4: (Cloud-gated) Output parity**

If cloud access is available, diff the final `spectronaut_output.zip` manifest (file list) against a pre-change baseline for the same inputs. Expected: same set of output files. If unavailable, mark **deferred** in the PR description.

- [ ] **Step 5: Commit verification notes**

```bash
git commit --allow-empty -m "test: record e2e behavior-parity verification for helper refactor"
```

---

## Self-Review

**1. Spec coverage:**
- M1 (helper extraction) → Tasks 1–3 (create helpers), Task 4 (bake into image), Tasks 5–9 (adopt in all six Spectronaut-image tasks). ✓
- S1 (single-call parallel download) → Task 3 (`sn_download_inputs`), adopted in Tasks 5/7/8. ✓
- v21.0-only image scope → Task 4 + Global Constraints. ✓
- Non-Spectronaut tasks untouched → Global Constraints; not referenced in any refactor task. ✓
- Byte-identical behavior gate → Task 1 test, Task 10 Steps 1–3. ✓
- Coupled release → Global Constraints + Task 4 Step 4 note. ✓
- R1/R2 dropped → not present in any task (correct). ✓

**2. Placeholder scan:** Tasks 6/7/8/9 Step 1 instruct reading the current task and copying its `spectronaut` invocation **verbatim** rather than reproducing 200+ lines four times. This is deliberate — the invocations are long, task-specific, and already correct in the file; paraphrasing them risks flag drift. The head/tail helper pattern IS fully shown (Task 5). This is a judgment call, not a lazy placeholder: the transformation is identical across tasks and shown once concretely; only the preserved-verbatim middle differs per task. Acceptable.

**3. Type consistency:** Function names are consistent across all tasks: `sn_monitor_start`, `sn_monitor_report <cpu> <disk> <root>` (3 args everywhere), `sn_download_inputs <dest> <paths_file>`, `sn_download_libraries <dest> <paths_file>` (returns arg string), `sn_build_import_flags` (reads `ENZYME_DB`/`MOD_REPO`). Env-var names `ENZYME_DB`/`MOD_REPO` consistent between Task 2 helper and Tasks 5–9 producers. ✓

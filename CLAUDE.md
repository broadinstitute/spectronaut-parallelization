# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This repository contains WDL (Workflow Description Language) workflows and Docker build infrastructure for running Spectronaut mass spectrometry proteomics analysis on Terra/Cromwell cloud platforms. The workflows are designed for the Broad Institute Proteogenomics team.

## Repository Structure

```
spectronaut-parallelization/
├── src/
│   ├── docker/                          # Templated Dockerfile for Spectronaut images
│   │   ├── panoply_spectronaut.dockerfile
│   │   └── helpers/                     # Shared task helpers, COPYed into the image
│   │       ├── sn_resource_monitor.sh
│   │       ├── sn_download.sh
│   │       ├── sn_import_flags.sh
│   │       └── tests/                   # Run directly with bash; no framework
│   └── spectronaut-installer/           # Spectronaut .deb installer (Git LFS)
│       └── Spectronaut_21.0.260602.94842.deb
├── wdl_workflow/
│   ├── parallelized_search/             # Main parallel workflow
│   │   └── parallel_spectronaut.wdl
│   ├── regular_directDIA/               # Non-parallelized single-VM workflow
│   │   └── spectronaut_directDIA_v20.wdl
│   └── spectronaut_modules/             # Standalone reusable modules
│       └── sne_combine.wdl
└── docs/                                # Technical documentation
    └── biognosys/                       # Vendor PDFs
```

**Note:** All files under `src/` are tracked by Git LFS — **except** `src/docker/helpers/**/*.sh`, which `.gitattributes` explicitly excludes. Keep that exclusion: the Dockerfile `COPY`s these scripts directly, so if LFS captured them the image would receive 3-line pointer files instead of shell code and every task would fail. Verify with `git check-attr filter src/docker/helpers/sn_download.sh` — it must not report `lfs`.

## Docker Images

| Dockerfile | Spectronaut Version | Image Tag |
|------------|-------------------|-----------|
| `panoply_spectronaut.dockerfile` | 21.0.260602.94842 | `broadcptacdev/panoply_spectronaut:v21.0` |

`panoply_spectronaut.dockerfile` is a template: before building, point its `COPY` at the desired `.deb` under `src/spectronaut-installer/` and replace the `<spectronaut-license-key>` placeholder in the `spectronaut activate` directive. The image is based on `ubuntu:22.04` and includes google-cloud-cli, dotnet-sdk-8.0, and Spectronaut installed via `.deb`. The license is baked in at build time.

**Older image tags (`v19.7`, `v20.3`–`v20.5`) remain published on Docker Hub** — `v20.5` is still referenced by `spectronaut_directDIA_v20.wdl` — but their Dockerfiles and installers are no longer kept in this repo, so those tags can be pulled but not rebuilt from source here. To rebuild one, retrieve the matching `.deb` from Biognosys and adjust the template's `COPY`.

**Rebuilding `v21.0` is mandatory when the `sn_*` helpers change.** `parallel_spectronaut.wdl` sources the helper scripts from `/usr/local/bin/` inside the image, so the WDL and the image must ship together — a task run against an image lacking them fails at its `source` line. The tag is mutable, so the rebuilt image must be pushed before the workflow is run.

**Build context is the repository root** — the Dockerfile `COPY`s `src/spectronaut-installer/...`, so build from the repo root with `-f`, not from the `src/docker/` directory:
```bash
docker buildx build \
  --platform linux/amd64 \
  -t broadcptacdev/panoply_spectronaut:v21.0 \
  -f src/docker/panoply_spectronaut.dockerfile \
  --push .
```
The `--platform linux/amd64` flag is required when cross-compiling from Apple Silicon.

## Workflow Variants

### 1. Parallelized Search (`wdl_workflow/parallelized_search/parallel_spectronaut.wdl`)

The main production workflow. Uses `broadcptacdev/panoply_spectronaut:v21.0`. The license is hardcoded in the Docker image; no license-key input is required. Uses `version development` WDL (required for `Directory` type support).

### 2. Regular DirectDIA (`wdl_workflow/regular_directDIA/`)

Single-VM non-parallelized workflow:
- **`spectronaut_directDIA_v20.wdl`** — Uses `broadcptacdev/panoply_spectronaut:v20.5`

Uses `version development` WDL. Flow: `list_files` → optional scatter(`convert_htrms` per file) → `spectronaut` (single VM search). Does not source the `sn_*` helpers.

### 3. SNE Combine Module (`wdl_workflow/spectronaut_modules/sne_combine.wdl`)

Standalone reusable module for combining SNE result files. Uses `version 1.0` WDL. Docker image: `broadcptacdev/panoply_spectronaut:v21.0`. Does not source the `sn_*` helpers, though it shares the `v21.0` image with the parallel workflow.

- **`produce_final_sne=true`** (default): `spectronaut manageSNE --merge` — generates final merged SNE with reports
- **`produce_final_sne=false`**: `spectronaut combine` — combines SNE files without full report generation
- Dynamic RAM: `ram_per_gb_sne * total_sne_gb` (floor 64 GB; 5.0 GB/GB for final, 3.0 GB/GB otherwise)

## Parallelized Search Workflow Architecture

### Execution Flow

```
Phase 1: Discovery
  list_files → validate_skip_pulsar

Phase 2: HTRMS Conversion (optional, per file scatter)
  htrms_conversion → sum_floats (calculate total size for disk allocation)

Phase 3A: Single VM (num_vms == 1)
  directDIA_single_vm (runs spectronaut direct OR diaanalysis)

Phase 3B: Multi-VM Pulsar Pipeline (num_vms > 1, do_pulsar=true)
  scatter(pulsar_step1_binned)       # Intermediate search archives (.psar) per bin
  → pulsar_step2_combine_models      # Train optimized models (.qsp)
  → scatter(pulsar_step3_binned)     # Final search archives with optimized models
  → combine_final_archives           # Merge into .kit library

Phase 3B (do_pulsar=false): Multi-VM with User Libraries
  (Pulsar steps 1.1–1.4 skipped; user provides spectral_library_1/2/3)

Phase 4: DIA Analysis (multi-VM only)
  scatter(dia_analysis_binned)       # DIA analysis per bin → .sne files

Phase 5: Merge
  combine_sne                        # Merge SNE files + generate reports → spectronaut_output.zip
```

### Task Reference

| Task | Docker Image | Function |
|------|-------------|---------|
| `validate_skip_pulsar` | `ubuntu:22.04` | Validates do_pulsar + library configuration |
| `list_files` | `gcr.io/google.com/cloudsdktool/cloud-sdk:stable` | Lists files in GCS directory |
| `create_bins` | `python:3.9-slim` | Round-robin binning (max 80 VMs) |
| `sum_floats` | `python:3.9-slim` | Sums HTRMS file sizes for disk allocation |
| `htrms_conversion` | `broadcptacdev/panoply_spectronaut:v21.0` | Per-file raw → HTRMS conversion |
| `directDIA_single_vm` | `broadcptacdev/panoply_spectronaut:v21.0` | Single-VM search (num_vms=1) |
| `pulsar_step1_binned` | `broadcptacdev/panoply_spectronaut:v21.0` | Intermediate .psar per bin |
| `pulsar_step2_combine_models` | `broadcptacdev/panoply_spectronaut:v21.0` | Train optimized .qsp models |
| `pulsar_step3_binned` | `broadcptacdev/panoply_spectronaut:v21.0` | Final .psar per bin with optimized models |
| `combine_final_archives` | `broadcptacdev/panoply_spectronaut:v21.0` | Merge archives → .kit library |
| `dia_analysis_binned` | `broadcptacdev/panoply_spectronaut:v21.0` | DIA analysis per bin → .sne files |
| `combine_sne` | `broadcptacdev/panoply_spectronaut:v21.0` | Merge SNE + reports → output.zip |

### Workflow Inputs

**Required:**
- `num_vms` (Int): Number of VMs; set to 1 for single-VM mode
- `experiment_name` (String): Name for the experiment
- `file_directory` (String): GCS path to input files (gs://...)
- `fasta_1` (File): Primary FASTA database

**Optional:**
- `fasta_2`, `fasta_3` (File?): Additional FASTA databases
- `do_conversion` (Boolean, default: true): Convert raw files to HTRMS before search
- `do_pulsar` (Boolean, default: true): Use Pulsar library generation; set false to use user-provided libraries
- `spectral_library_1/2/3` (String?): GCS paths to user spectral libraries (required if `do_pulsar=false`)
- `experiment_type` (String, default: "proteome"): `"proteome"` or `"ptm"` — controls resource presets
- `average_file_size_gb` (Float, default: 5.0): Used for disk sizing when HTRMS conversion is skipped
- `disk_size_multiplier` (Float, default: 3.0): Multiplier for raw/HTRMS disk allocation
- `sne_combine_disk_size_multiplier` (Float, default: 5.0): Multiplier for SNE combine disk
- `generate_sne_large_experiment` (Boolean, default: false): selects the `combine_sne` mode. **`true`** → `spectronaut manageSNE --merge` (full merge with reports) using the higher merge RAM presets; **`false`** (default) → `spectronaut combine`. Note the flag name reads backwards relative to what it does — `true` selects the heavier merge path, not a lightweight large-experiment path.
- `enzyme_database` (File?): Custom enzyme database (imported via `--importEnzymeDB`)
- `custom_mod_repository` (File?): Custom modification repository (imported via `--importModRepository`)
- `convert_schema` (File?): Schema for HTRMS conversion
- `directDIA_settings` (File?): Settings for search archive generation
- `DIA_analysis_settings` (File?): Settings for DIA analysis
- `condition_setup` (File?): Condition setup file
- `report_schema_1` through `report_schema_4` (File?): Report schema files
- `json_settings` (File?): JSON settings file

**Preemptible settings** (all Int; defaults shown):
- `n_preemptible_htrms_conversion` (default: 2)
- `n_preemptible_pulsar_step1` (default: 1)
- `n_preemptible_pulsar_step2` (default: 0)
- `n_preemptible_pulsar_step3` (default: 1)
- `n_preemptible_directDIA_single_vm` (default: 0)
- `n_preemptible_combine_archives` (default: 0)
- `n_preemptible_dia_analysis` (default: 1)
- `n_preemptible_combine_sne` (default: 0)

### Resource Presets by `experiment_type`

CPU and RAM are set via Map lookups keyed on `experiment_type`. RAM for dynamic tasks scales per file count (GB/file with floor and cap).

| Task | proteome CPU | proteome RAM | ptm CPU | ptm RAM |
|------|-------------|-------------|---------|---------|
| `pulsar_step1_binned` | 50 | 128 GB | 64 | 156 GB |
| `pulsar_step2_combine_models` | 25 | dynamic (1.0 GB/file) | 32 | dynamic (1.2 GB/file) |
| `pulsar_step3_binned` | 40 | 96 GB | 50 | 128 GB |
| `combine_final_archives` | 25 | dynamic (1.0 GB/file) | 32 | dynamic (1.2 GB/file) |
| `directDIA_single_vm` | 64 | 128 GB | 128 | 256 GB |
| `dia_analysis_binned` | 35 | 96 GB | 45 | 128 GB |
| `combine_sne` | 25 | dynamic (5.0 merge / 3.0 combine GB/file) | 30 | dynamic (6.0 merge / 4.0 combine GB/file) |

`htrms_conversion` is not preset-driven: it is fixed at 16 CPU / 32 GB / 300 GB disk.

Dynamic RAM is capped at `dynamic_ram_cap_gb` = **700 GB** (chosen against Compute Engine machine-type limits). Floors differ by task: `pulsar_step2_combine_models` and `combine_final_archives` floor at **128 GB**; `combine_sne` floors at **64 GB**. `combine_sne`'s per-file rate depends on `generate_sne_large_experiment` — the merge presets when true, the combine presets when false.

**CPU Platform:** none is pinned. Each task carries a commented-out `# cpuPlatform: "AMD Milan"` line; uncomment it to constrain placement.

## Key Implementation Patterns

**Shared Task Helpers (`sn_*`) — baked into the image:**
Common task logic lives in three scripts under `src/docker/helpers/`, `COPY`ed to `/usr/local/bin/` in the image and `source`d by each Spectronaut task in `parallel_spectronaut.wdl`. They are sourced, not executed, so they define functions rather than run:

| Script | Functions | Purpose |
|--------|-----------|---------|
| `sn_resource_monitor.sh` | `sn_monitor_start`, `sn_monitor_report <cpu> <disk_gb> <root>` | cgroup v1/v2 CPU/memory/disk report |
| `sn_download.sh` | `sn_download_inputs <dest> <paths_file>`, `sn_download_libraries <dest> <paths_file>` | GCS downloads |
| `sn_import_flags.sh` | `sn_build_import_flags` | Builds the `--importEnzymeDB` / `--importModRepository` prefix |

Task pattern: `set -euo pipefail` → `source` the helpers → `sn_monitor_start` → `sn_download_inputs` → build flags → `spectronaut …` → `sn_monitor_report`.

Conventions to preserve when editing:
- **`sn_build_import_flags` reads the environment**, not arguments. A task must `export ENZYME_DB=…` and `export MOD_REPO=…` (empty string when the `File?` input is undefined) *before* calling it.
- **Interpolate `${import_flags}` unquoted.** Word-splitting is what turns the string into separate argv entries; quoting it passes one argument and breaks the command. Same applies to `${cmd_flags}` and `${user_lib_args}`. The resulting shellcheck SC2086 warnings are expected.
- **`sn_download_inputs` collapses the per-file loop into one `gcloud storage cp -r --read-paths-from-stdin` call** so gcloud parallelizes across sources. It hard-fails if fewer top-level entries land than paths requested — a partial download would otherwise silently search a subset of the data. It falls back to a serial per-file loop when gcloud lacks the flag.
- **`sn_download_libraries` writes its `-a` flags to stdout** and all progress to stderr; the caller captures it via `user_lib_args="$(…)"`. Keep progress output on stderr or it will corrupt the captured flags.
- `htrms_conversion` intentionally does not source the helpers: it has no resource monitor and downloads a single file.

Tests are plain bash with a fake `gcloud` on `PATH` — no framework. Run them directly after editing a helper:
```bash
for t in src/docker/helpers/tests/*.sh; do bash "$t"; done
```

**File Management:**
- Uses `mktemp -d` within Cromwell execution directory to avoid root filesystem space issues
- Pattern: `cromwell_root=$(pwd)` → `sn_temp=$(mktemp -d sn_temp_XXXXXX)` → `working_dir=$(mktemp -d working_dir_XXXXXX)`

**Input Localization — `localizationOptional` (Cromwell-specific):**
Tasks that receive raw input data paths (`directDIA_single_vm`, `pulsar_step1_binned`, `pulsar_step3_binned`, `dia_analysis_binned`) and GCS resources (`htrms_conversion` input, `user_spectral_libraries`) declare those inputs as `String`/`Array[String]` and download them **inside the command** with `gcloud storage cp -r`, rather than letting Cromwell localize them as `File`:
```wdl
parameter_meta {
    input_files: { localizationOptional: true }
}
```
- **Why required:** timsTOF `.d` inputs are **directories** (GCS prefixes), not single objects. Cromwell's `File` localizer cannot localize a directory, so declaring these as `Array[File]` fails with "cannot find files" when `do_conversion=false`. `gcloud storage cp -r` handles both `.d` directories and `.htrms` files.
- **Caveat — NOT portable WDL.** `parameter_meta` is standard (and accepts arbitrary keys, so this parses everywhere), but the `localizationOptional` *key* is a **Cromwell convention**, honored only on Cromwell/Terra. The WDL 1.2 spec standardizes this differently as a `localization_optional` task **hint** (snake_case, in a `hints {}` section). Since download happens in-command regardless, ignoring the key on a non-Cromwell engine is harmless here.
- Inputs flow as GCS path strings end-to-end: `list_files` → `create_bins` (`bins_json`) → search tasks. When `do_conversion=true`, `htrms_conversion` outputs are Cromwell `File`s whose serialized values are `gs://` paths, so `gcloud storage cp -r` works for them too.

**Dynamic Disk Sizing:**
- `htrms_conversion`: `ceil(file_size_gb * disk_size_multiplier)`
- Downstream tasks: propagate measured file sizes from `sum_floats`
- `combine_sne`: separate `sne_combine_disk_size_multiplier`

**Resource Monitoring:**
- Every task emits a resource usage report at completion via cgroup v1/v2 (CPU, memory, disk)
- Pattern: reads `/sys/fs/cgroup/memory.peak` or `/sys/fs/cgroup/memory/memory.max_usage_in_bytes`

**Enzyme Database & Modification Repository Import:**
Custom enzyme databases and modification repositories are imported inline as flags on the Spectronaut command (prepended before the action), gated by `defined()`:
```
~{if defined(enzyme_database) then "--importEnzymeDB " + enzyme_database else ""}
~{if defined(custom_mod_repository) then "--importModRepository " + custom_mod_repository else ""}
```

**HTRMS Condition Setup:**
When converting to HTRMS, update the `condition_setup` file Run Label column extensions to `.htrms`.

**Optional File Handling:**
Optional files use WDL `File?` type with conditional inclusion via `~{if defined(x) then "-flag ~{x}" else ""}`.

## Spectronaut Command Patterns

### HTRMS Conversion
```bash
spectronaut -convert \
  -i <input_folder> \
  -o <output_folder> \
  [-s <convert_schema>]
```

### DirectDIA Search Archive Generation (Pulsar Step 1 & 3)
```bash
spectronaut direct \
  -d <input_folder> \
  -fasta <fasta_file> \
  [-s <directDIA_settings>] \
  -o <output_dir> \
  -setTemp <temp_dir>
```

### Archive Merging (Pulsar Step 2 — combine models)
```bash
spectronaut lg -se Pulsar \
  -sad <archives_directory> \
  -k <output_kit_file> \
  -o <output_dir>
```

### Archive Merging (combine final archives → .kit)
```bash
spectronaut lg -se Pulsar \
  -sad <archives_directory> \
  -k <output_kit_file> \
  -o <output_dir>
```

### DIA Analysis
```bash
spectronaut diaanalysis \
  [-s <DIA_analysis_settings>] \
  -fasta <fasta_file> \
  [-j <json_settings>] \
  -n <experiment_name> \
  -o <output_dir> \
  -d <data_folder> \
  -a <search_archive_or_library> \
  -setTemp <temp_dir>
```

### SNE Merge (Final Output)
```bash
spectronaut manageSNE --merge \
  -n <experiment_name> \
  -o <output_dir> \
  -d <sne_directory> \
  [-con <condition_setup>] \
  [-s <analysis_settings>] \
  [-rs <report_schema>]
```

### SNE Combine (Large Experiment Mode)
```bash
spectronaut combine \
  -n <experiment_name> \
  -d <sne_directory> \
  [-fasta <fasta_file>] \
  [-rs <report_schema>] \
  -o <output_dir>
```

### Single-VM DirectDIA Search (regular_directDIA workflows)
```bash
spectronaut \
  [-direct] \
  [-s <analysis_settings>] \
  [-con <condition_setup>] \
  -fasta <fasta_file> \
  [-a <spectral_library>] \
  [-rs <report_schema>] \
  [-j <json_settings>] \
  -n <experiment_name> \
  -o <output_dir> \
  -d <data_folder> \
  -setTemp <temp_dir>
```

## WDL Version Notes

| Workflow | WDL Version | Reason |
|----------|------------|--------|
| `parallel_spectronaut.wdl` | `development` | Requires `Directory` type |
| `spectronaut_directDIA_*.wdl` | `development` | Requires `Directory` type |
| `sne_combine.wdl` | `1.0` | No `Directory` type needed |

## File Transfer (gcloud CLI)

Authenticate once: `gcloud auth login`

```bash
# Copy single file
gcloud storage cp <source> <destination>

# Copy multiple files
gcloud storage cp <source-1> <source-2> ... <destination>

# Copy folder recursively
gcloud storage cp --recursive <source> <destination>
```

No need to set project ID before transfers between buckets.

## Post-Edit Requirements

After editing any WDL file, convert to Unix line endings:
```bash
dos2unix <file.wdl>
```

## Contact

- Author: C. Lian, D. R. Mani
- Email: proteogenomics@broadinstitute.org, glian@broadinstitute.org

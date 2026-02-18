# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This repository contains WDL (Workflow Description Language) workflows and Docker build infrastructure for running Spectronaut mass spectrometry proteomics analysis on Terra/Cromwell cloud platforms. The workflows are designed for the Broad Institute Proteogenomics team.

## Repository Structure

```
panoply-workflow-dev/
├── src/
│   ├── docker/                          # Dockerfiles for Spectronaut images
│   │   ├── spectronaut_v19.7.dockerfile
│   │   ├── spectronaut_v20.3.dockerfile
│   │   ├── spectronaut_v20.4.dockerfile
│   │   └── spectronaut_v20.4_public.dockerfile
│   └── spectronaut-installer/           # Spectronaut .deb installers (Git LFS)
│       ├── Spectronaut_19.7.250203.62635.deb
│       ├── Spectronaut_20.1.250624.92449.deb
│       ├── Spectronaut_20.2.250922.92449.deb
│       ├── Spectronaut_20.3.251119.92449.deb
│       └── Spectronaut_20.4.260109.92449.deb
├── wdl_workflow/
│   ├── parallelized_search/             # Main parallel workflows
│   │   ├── parallel_spectronaut_private.wdl  # Hardcoded license (internal use)
│   │   └── parallel_spectronaut_open.wdl     # Runtime license key (public use)
│   ├── regular_directDIA/               # Non-parallelized single-VM workflows
│   │   ├── spectronaut_directDIA_v19.7.wdl
│   │   └── spectronaut_directDIA_v20.4.wdl
│   └── spectronaut_modules/             # Standalone reusable modules
│       └── sne_combine.wdl
└── doc/                                 # Technical documentation and PDFs
```

**Note:** All files under `src/` are tracked by Git LFS.

## Docker Images

| Dockerfile | Spectronaut Version | Image Tag | License |
|------------|-------------------|-----------|---------|
| `spectronaut_v19.7.dockerfile` | 19.7.250203.62635 | `broadcptacdev/panoply_spectronaut:v19.7` | Hardcoded at build |
| `spectronaut_v20.3.dockerfile` | 20.3.251119.92449 | `broadcptacdev/panoply_spectronaut:v20.3` | Hardcoded at build |
| `spectronaut_v20.4.dockerfile` | 20.4.260109.92449 | `broadcptacdev/panoply_spectronaut:v20.4` | Hardcoded at build |
| `spectronaut_v20.4_public.dockerfile` | 20.4.260109.92449 | `broadcptacdev/panoply_spectronaut_public:v20.4` | None (activated at runtime) |

All images are based on `ubuntu:22.04` and include google-cloud-cli, dotnet-sdk-8.0, and Spectronaut installed via `.deb`.

Build command (cross-compile from Apple Silicon):
```bash
docker buildx build --platform linux/amd64 -f <dockerfile> -t <image_tag> .
```

## Workflow Variants

### 1. Parallelized Search (`wdl_workflow/parallelized_search/`)

The main production workflows. Both variants are functionally identical except for license handling:

- **`parallel_spectronaut_private.wdl`** — License hardcoded in Docker image; no license input required. Uses `broadcptacdev/panoply_spectronaut:v20.4`.
- **`parallel_spectronaut_open.wdl`** — Requires `spectronaut_license_key` String input. Uses `broadcptacdev/panoply_spectronaut_public:v20.4`. Every Spectronaut task runs `spectronaut activate "~{spectronaut_license_key}"` at the start.

Both use `version development` WDL (required for `Directory` type support).

### 2. Regular DirectDIA (`wdl_workflow/regular_directDIA/`)

Single-VM non-parallelized workflows:
- **`spectronaut_directDIA_v19.7.wdl`** — Uses `broadcptacdev/panoply_spectronaut:v19.7`
- **`spectronaut_directDIA_v20.4.wdl`** — Uses `broadcptacdev/panoply_spectronaut:v20.4`

Both use `version development` WDL. Flow: `list_files` → optional scatter(`convert_htrms` per file) → `spectronaut` (single VM search).

### 3. SNE Combine Module (`wdl_workflow/spectronaut_modules/sne_combine.wdl`)

Standalone reusable module for combining SNE result files. Uses `version 1.0` WDL. Docker image: `broadcptacdev/panoply_spectronaut:v20.4`.

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
| `validate_skip_pulsar` | `python:3.9-slim` | Validates do_pulsar + library configuration |
| `list_files` | `google/cloud-sdk:slim` | Lists files in GCS directory |
| `create_bins` | `python:3.9-slim` | Round-robin binning (max 80 VMs) |
| `sum_floats` | `python:3.9-slim` | Sums HTRMS file sizes for disk allocation |
| `htrms_conversion` | `broadcptacdev/panoply_spectronaut:v20.4` | Per-file raw → HTRMS conversion |
| `directDIA_single_vm` | `broadcptacdev/panoply_spectronaut:v20.4` | Single-VM search (num_vms=1) |
| `pulsar_step1_binned` | `broadcptacdev/panoply_spectronaut:v20.4` | Intermediate .psar per bin |
| `pulsar_step2_combine_models` | `broadcptacdev/panoply_spectronaut:v20.4` | Train optimized .qsp models |
| `pulsar_step3_binned` | `broadcptacdev/panoply_spectronaut:v20.4` | Final .psar per bin with optimized models |
| `combine_final_archives` | `broadcptacdev/panoply_spectronaut:v20.4` | Merge archives → .kit library |
| `dia_analysis_binned` | `broadcptacdev/panoply_spectronaut:v20.4` | DIA analysis per bin → .sne files |
| `combine_sne` | `broadcptacdev/panoply_spectronaut:v20.4` | Merge SNE + reports → output.zip |

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
- `generate_sne_large_experiment` (Boolean, default: false): Use `spectronaut combine` instead of `manageSNE --merge` for combine_sne
- `enzyme_database` (File?): Custom enzyme database
- `convert_schema` (File?): Schema for HTRMS conversion
- `directDIA_settings` (File?): Settings for search archive generation
- `DIA_analysis_settings` (File?): Settings for DIA analysis
- `condition_setup` (File?): Condition setup file
- `report_schema_1` through `report_schema_4` (File?): Report schema files
- `json_settings` (File?): JSON settings file
- `spectronaut_license_key` (String): **open variant only** — Spectronaut license key

**Preemptible settings** (all Int, default: 0):
- `n_preemptible_htrms_conversion`
- `n_preemptible_pulsar_step1`
- `n_preemptible_pulsar_step2`
- `n_preemptible_pulsar_step3`
- `n_preemptible_combine_archives`
- `n_preemptible_dia_analysis`
- `n_preemptible_combine_sne`

### Resource Presets by `experiment_type`

CPU and RAM are set via Map lookups keyed on `experiment_type`. RAM for dynamic tasks scales per file count (GB/file with floor and cap).

| Task | proteome CPU | proteome RAM | ptm CPU | ptm RAM |
|------|-------------|-------------|---------|---------|
| `pulsar_step1_binned` | 64 | 150 GB | 64 | 200 GB |
| `pulsar_step2_combine_models` | 32 | dynamic (0.8 GB/file, floor 64) | 48 | dynamic (1.2 GB/file) |
| `pulsar_step3_binned` | 64 | 150 GB | 128 | 200 GB |
| `combine_final_archives` | 32 | dynamic (1.2 GB/file, floor 64) | 48 | dynamic (1.8 GB/file) |
| `directDIA_single_vm` | 64 | 128 GB | 128 | 256 GB |
| `dia_analysis_binned` | 64 | 150 GB | 64 | 200 GB |
| `combine_sne` | 32 | dynamic (5.0 or 3.0 GB/SNE-GB) | 32 | same |

All dynamic RAM values have a floor of 64 GB and a cap of 750 GB.

**CPU Platform:** All parallel workflow tasks use `cpuPlatform: "Intel Cascade Lake"`.

## Key Implementation Patterns

**File Management:**
- Uses `mktemp -d` within Cromwell execution directory to avoid root filesystem space issues
- Pattern: `cromwell_root=$(pwd)` → `sn_temp=$(mktemp -d sn_temp_XXXXXX)` → `working_dir=$(mktemp -d working_dir_XXXXXX)`

**Dynamic Disk Sizing:**
- `htrms_conversion`: `ceil(file_size_gb * disk_size_multiplier)`
- Downstream tasks: propagate measured file sizes from `sum_floats`
- `combine_sne`: separate `sne_combine_disk_size_multiplier`

**Resource Monitoring:**
- Every task emits a resource usage report at completion via cgroup v1/v2 (CPU, memory, disk)
- Pattern: reads `/sys/fs/cgroup/memory.peak` or `/sys/fs/cgroup/memory/memory.max_usage_in_bytes`

**Enzyme Database Import:**
Custom enzyme databases require a separate import step before search:
```bash
dotnet /usr/lib/spectronaut/SpectronautCMD.dll --importEnzymeDB <database>
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
| `parallel_spectronaut_*.wdl` | `development` | Requires `Directory` type |
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

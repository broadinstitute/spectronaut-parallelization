# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This repository contains WDL (Workflow Description Language) workflows for running Spectronaut mass spectrometry proteomics analysis on Terra/Cromwell cloud platforms. The workflows are designed for the Broad Institute Proteogenomics team and use the `broadcptacdev/panoply_spectronaut` Docker images.

## Workflow Architecture

### Main Workflow Variants

1. **panoply_spectronaut_parallel.wdl** - Main parallel workflow (in development)
   - Supports HTRMS file conversion and Spectronaut search in parallel
   - Uses boolean flags `do_conversion` and `do_search` to control execution
   - Currently incomplete/under development

2. **HTRMSConversion_support/** - HTRMS conversion and search workflows
   - `Spectronaut_v19-7.wdl` - Spectronaut v19.7 with HTRMS conversion
   - `Spectronaut_v20-0.wdl` - Spectronaut v20.0 with HTRMS conversion (current)
   - Two-stage workflow: convert_htrms task → spectronaut task
   - Uses WDL `development` version (requires Directory input type support)

3. **SNECombine_support/spectronaut20.wdl** - SNE file combination
   - Combines multiple SNE result files or directories
   - Uses `spectronaut -combine` command

4. **generate_report/generate_report_v20-0.wdl** - Report generation
   - Generates reports from existing SNE files
   - Uses `spectronaut manageSNE` command

5. **spectronaut-parallel/parallel_spectronaut_v5_bin.wdl** - Dynamic VM binning workflow
   - Distributes files across n VMs using round-robin binning
   - Calculates disk sizes dynamically based on actual input file sizes
   - Uses WDL `development` version
   - Docker image: `cameronlian/panoply-spectronaut:v20.2`

### Workflow Execution Flow

For the main HTRMS conversion workflow (HTRMSConversion_support/Spectronaut_v20-0.wdl):

1. If `do_conversion=true`: convert_htrms task runs
   - Converts raw files to HTRMS format using `spectronaut -convert`
   - Outputs to `htrms_converted` directory

2. If `do_search=true`: spectronaut task runs
   - Takes either converted HTRMS files or raw files as input
   - Runs Spectronaut search (DirectDIA or library-based)
   - Outputs zipped results to `spectronaut_output.zip`

### Key Implementation Patterns

**File Management Strategy:**
- Uses temporary directories within Cromwell root filesystem to avoid root fs space issues
- Pattern: `cromwell_root=$(pwd)` → create temp dirs → work in temp → move outputs back
- Spectronaut temp directory: `sn_temp=$(mktemp -d sn_temp_XXXXXX)`
- Working directory: `working_dir=$(mktemp -d working_dir_XXXXXX)`

**DirectDIA vs Library-Based Search:**
- If no `spectral_library` defined: DirectDIA mode (`spectronaut -direct`)
- If `spectral_library` provided: Library-based search (`spectronaut -a <library>`)

**Docker Images:**
- v19.7: `broadcptacdev/panoply_spectronaut:v19.7`
- v20.0: `broadcptacdev/panoply_spectronaut:v20.0` (current)
- v20.2: `cameronlian/panoply-spectronaut:v20.2` (spectronaut-parallel)

### Spectronaut Parallel Workflow Details

**Workflow Inputs:**

Required inputs:
- `num_vms`: Number of VMs for parallel processing
- `experiment_name`: Name for the experiment
- `file_directory`: GCS path to input files
- `fasta_1`: Primary FASTA database file

Optional inputs:
- `fasta_2`, `fasta_3`: Additional FASTA database files
- `disk_size_multiplier`: Multiplier for disk size calculation (default: 3)
- `do_conversion`: Convert raw files to HTRMS (default: true)
- `enzyme_database`: Custom enzyme database file
- `convert_schema`: Schema for HTRMS conversion
- `directDIA_settings`: Settings for search archive generation
- `DIA_analysis_settings`: Settings for DIA analysis against merged library
- `condition_setup`: Condition setup file
- `report_schema_1` through `report_schema_4`: Report schema files
- `json_settings`: JSON settings file
- `experiment_type`: "proteome" or "ptm" (default: "proteome")

Preemptible settings (all default to 0):
- `n_preemptible_htrms_conversion`
- `n_preemptible_directDIA_search`
- `n_preemptible_combine_archives`
- `n_preemptible_dia_analysis`
- `n_preemptible_combine_sne`

**Resource Presets by experiment_type:**

| Task | proteome CPU | proteome RAM | ptm CPU | ptm RAM |
|------|--------------|--------------|---------|---------|
| directDIA_search | 80 | 256 GB | 128 | 512 GB |
| combine_archives | 16 | 384 GB | 16 | 640 GB |
| dia_analysis | 32 | 128 GB | 64 | 256 GB |
| combine_sne | 16 | 384 GB | 16 | 640 GB |

**Parallel Workflow Execution Flow:**

1. **list_files**: Lists all files in GCS directory
2. **create_bins**: Distributes files across VMs using round-robin algorithm
3. **First scatter** (per bin):
   - `download_and_size_binned`: Downloads files from GCS, calculates total size for dynamic disk allocation
   - `htrms_conversion_binned` (if do_conversion=true): Converts raw files to HTRMS format
   - `directDIA_search_binned`: Generates search archives (.psar)
4. **combine_archives**: Merges all .psar files into merged_library.kit
5. **Second scatter** (per bin):
   - `dia_analysis_binned`: Runs DIA analysis against merged library, produces .sne files
6. **combine_sne**: Merges all SNE files and generates reports (output: spectronaut_output.zip)

**Key Tasks and Docker Images:**

| Task | Docker Image | Function |
|------|--------------|----------|
| list_files | google/cloud-sdk:slim | Lists files in GCS path |
| create_bins | python:3.9-slim | Python script for round-robin distribution |
| download_and_size_binned | google/cloud-sdk:slim | Downloads files, calculates size |
| htrms_conversion_binned | cameronlian/panoply-spectronaut:v20.2 | Converts raw to HTRMS |
| directDIA_search_binned | cameronlian/panoply-spectronaut:v20.2 | Generates search archives |
| combine_archives | cameronlian/panoply-spectronaut:v20.2 | Merges archives into .kit library |
| dia_analysis_binned | cameronlian/panoply-spectronaut:v20.2 | DIA analysis against merged library |
| combine_sne | cameronlian/panoply-spectronaut:v20.2 | Merges SNE files, generates reports |

**Dynamic Disk Sizing:**
Disk sizes are calculated as `ceil(input_size_gb * disk_size_multiplier)`. The `download_and_size_binned` task measures actual file sizes, which propagate to downstream tasks for appropriate disk allocation.

## Spectronaut Command Patterns

### HTRMS Conversion
```bash
spectronaut -convert \
  -i <input_folder> \
  -o <output_folder> \
  [-s <convert_scheme>]  # Optional custom scheme
```

### Spectronaut Search
```bash
spectronaut \
  [-direct]                # DirectDIA mode (no library)
  [-s <analysis_settings>] # Analysis settings file
  [-con <condition_setup>] # Condition setup file
  -fasta <fasta_file>      # Required FASTA database
  [-a <spectral_library>]  # Spectral library (can specify multiple)
  [-rs <report_schema>]    # Report schema (can specify multiple)
  [-j <json_settings>]     # JSON settings
  -n <experiment_name>     # Experiment name
  -o <output_dir>          # Output directory
  -d <data_folder>         # Input data folder
  -setTemp <temp_dir>      # Temp directory
```

### SNE Combine
```bash
spectronaut -combine \
  -n <experiment_name> \
  [-sne <sne_file>]        # Individual SNE files (can specify multiple)
  [-d <sne_folder>]        # SNE folders (can specify multiple)
  -fasta <fasta_file>      # FASTA database(s)
  [-rs <report_schema>]    # Report schema(s)
  -o <output_dir>
```

### Generate Reports
```bash
spectronaut manageSNE \
  -sne <sne_file> \
  -o <output_dir> \
  -rs <report_schema_01> \
  [-rs <report_schema_02>] # Can specify up to 5 schemas
```

### Spectronaut Parallel Workflow Commands

DirectDIA search archive generation:
```bash
spectronaut direct \
  -d <input_folder> \
  -fasta <fasta_file> \
  [-s <analysis_settings>] \
  -o <output_dir> \
  -setTemp <temp_dir>
```

Archive merging:
```bash
spectronaut lg -se Pulsar \
  -sad <archives_directory> \
  -k <output_kit_file> \
  -o <output_dir>
```

DIA analysis:
```bash
spectronaut diaanalysis \
  [-s <analysis_settings>] \
  -fasta <fasta_file> \
  [-j <json_settings>] \
  -n <experiment_name> \
  -o <output_dir> \
  -d <data_folder> \
  -a <search_archive> \
  -setTemp <temp_dir>
```

SNE merging:
```bash
spectronaut manageSNE --merge \
  -n <experiment_name> \
  -o <output_dir> \
  -d <sne_directory> \
  [-con <condition_setup>] \
  [-s <analysis_settings>] \
  [-rs <report_schema>]
```

## Runtime Configurations

### Default Resource Allocation (snapshots 22-25, reduced from earlier versions)
- CPU: 16 cores
- Memory: 64 GB (896 GB max for AMD Rome)
- Disk: 2000 GB HDD (2 TB)
- Boot disk: 512 GB
- Platform: AMD Rome
- Preemptible: 0 (non-preemptible instances)

### Report Generation
- CPU: 16 cores
- Memory: 256 GB (configurable, default in generate_report)
- Disk: 1000 GB HDD

## Important Implementation Notes

1. **WDL Versions:**
   - Main parallel workflow uses `version 1.0` (standard)
   - Support workflows use `version development` (requires Directory type support)

2. **File Extension Requirements:**
   - When converting to HTRMS, update `condition_setup` file Run Label column extensions to ".htrms"

3. **Snapshot Versioning:**
   - Terra does not track snapshot versions
   - Recommend noting snapshot version in job comments
   - Latest snapshots (24-25) include HTRMS conversion support
   - See HTRMSConversion_support/panoply_spectronaut_HTRMSConversion_documentation.md for version history

4. **Input File Handling:**
   - `files_folder` or `htrms_files_folder`: Directory input for data files
   - If `htrms_files_folder` is defined, conversion is skipped regardless of `do_conversion`
   - Optional files use WDL `File?` type with conditional inclusion via `~{if defined(...) then ...}`

5. **Enzyme Database Import:**
   - Custom enzyme databases require separate import step before search:
     ```bash
     dotnet /usr/lib/spectronaut/SpectronautCMD.dll --importEnzymeDB <database>
     ```

## File Transfer (gcloud CLI)

Authenticate once: `gcloud auth login`

Fast parallel transfer commands:
- Copy file: `gcloud storage cp <source> <destination>`
- Copy multiple files: `gcloud storage cp <source-1> <source-2> ... <destination>`
- Copy folder: `gcloud storage cp --recursive <source> <destination>`

No need to set project ID before transfers between buckets.

## Contact

- Author: C. Lian, D. R. Mani
- Email: proteogenomics@broadinstitute.org, glian@broadinstitute.org
- after editing any WDL file, use dos2unix <file> command to converting that WDL file into unix format
- Workflow scripts in this project are to be run on Terra (Cromwell file management environment on Google Cloud)
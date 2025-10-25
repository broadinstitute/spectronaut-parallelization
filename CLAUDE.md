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
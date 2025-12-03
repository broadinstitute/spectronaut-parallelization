version development

# Universal HTRMS Conversion with Conditional Search Modes
# Phase I: Each raw file converted to HTRMS in its own VM
# Phase II: HTRMS files binned across n VMs
# Phase III: Conditional execution - Single VM (num_vms=1) or Parallel (num_vms>1)

workflow parallel_spectronaut {
    input {
        # ============================================================================
        # Required Workflow Parameters
        # ============================================================================
        String experiment_name            # Experiment identifier
        String file_directory             # GCS path to raw input files
        File fasta_1                      # Primary FASTA database file (required)
        Int num_vms = 1                   # Number of VMs for parallel processing (1 = single VM, >1 = parallel)

        # ============================================================================
        # Workflow Configuration
        # ============================================================================
        String experiment_type = "proteome"  # Experiment type: "proteome" or "ptm" (affects resource presets)
        Boolean do_conversion = true         # Enable HTRMS conversion (always true in current version)

        # ============================================================================
        # Optional Database Files
        # ============================================================================
        File? fasta_2                     # Additional FASTA database file
        File? fasta_3                     # Additional FASTA database file
        File? enzyme_database             # Custom enzyme database

        # ============================================================================
        # Analysis Settings & Schemas
        # ============================================================================
        File? convert_schema              # Schema for HTRMS conversion
        File? search_settings             # Settings for directDIA search and DIA analysis
        File? json_settings               # JSON settings for Spectronaut
        File? condition_setup             # Experimental condition setup

        # ============================================================================
        # Report Schemas
        # ============================================================================
        File? report_schema_1             # Report schema 1
        File? report_schema_2             # Report schema 2
        File? report_schema_3             # Report schema 3
        File? report_schema_4             # Report schema 4

        # ============================================================================
        # Resource Configuration
        # ============================================================================
        Int disk_size_multiplier = 4              # Multiplier for dynamic disk size calculation
        Int htrms_conversion_disk_gb = 300        # Fixed disk size per VM for HTRMS conversion

        # Preemptible instance settings (0 = non-preemptible, >0 = number of preemptible attempts)
        Int n_preemptible_htrms_conversion = 0    # HTRMS conversion preemptible attempts
        Int n_preemptible_directDIA_search = 0    # DirectDIA search preemptible attempts
        Int n_preemptible_combine_archives = 0    # Archive combining preemptible attempts
        Int n_preemptible_dia_analysis = 0        # DIA analysis preemptible attempts
        Int n_preemptible_combine_sne = 0         # SNE combining preemptible attempts
    }

    # Compute preset configurations based on experiment_type
    Map[String, Int] directDIA_search_cpu_presets = {
        "proteome": 80,
        "ptm": 128,
    }
    Map[String, Int] directDIA_search_ram_gb_presets = {
        "proteome": 150,
        "ptm": 250,
    }

    Map[String, Int] combine_archives_cpu_presets = {
        "proteome": 16,
        "ptm": 16,
    }
    Map[String, Int] combine_archives_ram_gb_presets = {
        "proteome": 50,
        "ptm": 100,
    }

    Map[String, Int] dia_analysis_cpu_presets = {
        "proteome": 32,
        "ptm": 64,
    }
    Map[String, Int] dia_analysis_ram_gb_presets = {
        "proteome": 80,
        "ptm": 150,
    }

    Map[String, Int] combine_sne_cpu_presets = {
        "proteome": 16,
        "ptm": 16,
    }
    Map[String, Int] combine_sne_ram_gb_presets = {
        "proteome": 40,
        "ptm": 80,
    }

    # Validate experiment_type and fallback to "proteome" if invalid
    String validated_experiment_type = if (experiment_type == "proteome" || experiment_type == "ptm") then experiment_type else "proteome"

    # Look up values based on validated_experiment_type
    Int directDIA_search_cpu = directDIA_search_cpu_presets[validated_experiment_type]
    Int directDIA_search_ram_gb = directDIA_search_ram_gb_presets[validated_experiment_type]

    Int combine_archives_cpu = combine_archives_cpu_presets[validated_experiment_type]
    Int combine_archives_ram_gb = combine_archives_ram_gb_presets[validated_experiment_type]

    Int dia_analysis_cpu = dia_analysis_cpu_presets[validated_experiment_type]
    Int dia_analysis_ram_gb = dia_analysis_ram_gb_presets[validated_experiment_type]

    Int combine_sne_cpu = combine_sne_cpu_presets[validated_experiment_type]
    Int combine_sne_ram_gb = combine_sne_ram_gb_presets[validated_experiment_type]

    # ============================================================================
    # PHASE I: Discovery and Universal HTRMS Conversion (1 file per VM)
    # ============================================================================

    # List all raw files in the input directory
    call list_files {
        input:
            gcs_path = file_directory,
    }

    Array[String] raw_file_paths = read_lines(list_files.file_list)

    # Scatter: Convert each raw file to HTRMS in its own VM
    scatter (raw_file_path in raw_file_paths) {
        call convert_single_file_htrms {
            input:
                file_path = raw_file_path,
                disk_size_gb = htrms_conversion_disk_gb,
                convert_schema = convert_schema,
        }
    }

    # Gather all converted HTRMS files
    Array[File] all_htrms_files = convert_single_file_htrms.htrms_file

    # ============================================================================
    # PHASE II: Intelligent Binning
    # ============================================================================

    # Bin HTRMS files with sorting and validation
    call create_bins {
        input:
            htrms_files = all_htrms_files,
            num_bins = num_vms,
    }

    Array[Array[File]] file_bins = read_json(create_bins.bins_json)
    Int calculated_num_vms = create_bins.calculated_num_vms

    # ============================================================================
    # PHASE III: Conditional Execution - Single VM vs Parallel
    # ============================================================================

    # BRANCH A: Single VM Mode (num_vms == 1)
    if (calculated_num_vms == 1) {
        # Calculate total size of all HTRMS files for disk allocation
        call calculate_total_size {
            input:
                files = all_htrms_files,
        }

        # Run classic directDIA on all files in one VM
        call directDIA_single_vm {
            input:
                experiment_name = experiment_name,
                input_files = all_htrms_files,
                total_size_gb = calculate_total_size.total_size_gb,
                disk_size_multiplier = disk_size_multiplier,
                analysis_schema = search_settings,
                fasta_1 = fasta_1,
                fasta_2 = fasta_2,
                fasta_3 = fasta_3,
                enzyme_database = enzyme_database,
                json_settings = json_settings,
                condition_setup = condition_setup,
                report_schema_1 = report_schema_1,
                report_schema_2 = report_schema_2,
                report_schema_3 = report_schema_3,
                report_schema_4 = report_schema_4,
                cpu = directDIA_search_cpu,
                ram_gb = directDIA_search_ram_gb,
                n_preemptible = n_preemptible_directDIA_search,
        }
    }

    # BRANCH B: Parallel Mode (num_vms > 1)
    if (calculated_num_vms > 1) {
        # Scatter: Generate search archives for each bin
        scatter (i in range(length(file_bins))) {
            # Calculate size of files in this bin
            call calculate_total_size as calc_bin_size {
                input:
                    files = file_bins[i],
            }

            # DirectDIA search for search archive generation
            call directDIA_search_binned {
                input:
                    input_files = file_bins[i],
                    analysis_schema = search_settings,
                    fasta_1 = fasta_1,
                    fasta_2 = fasta_2,
                    fasta_3 = fasta_3,
                    enzyme_database = enzyme_database,
                    cpu = directDIA_search_cpu,
                    ram_gb = directDIA_search_ram_gb,
                    bin_index = i,
                    n_preemptible = n_preemptible_directDIA_search,
                    bin_size_gb = calc_bin_size.total_size_gb,
                    disk_size_multiplier = disk_size_multiplier,
            }
        }

        Array[File] all_archives = select_all(directDIA_search_binned.search_archive)
        Array[Float] bin_sizes = select_all(calc_bin_size.total_size_gb)

        # Sum all bin sizes for combine_archives disk allocation
        call sum_floats as sum_archive_sizes {
            input:
                values = bin_sizes,
        }

        # Combine scattered search archives into one
        call combine_archives {
            input:
                input_archives = all_archives,
                total_input_size_gb = sum_archive_sizes.total,
                disk_size_multiplier = disk_size_multiplier,
                cpu = combine_archives_cpu,
                ram_gb = combine_archives_ram_gb,
                enzyme_database = enzyme_database,
                n_preemptible = n_preemptible_combine_archives,
        }

        # Scatter: DIA analysis for each bin against the merged library
        scatter (i in range(length(file_bins))) {
            call dia_analysis_binned {
                input:
                    experiment_name = experiment_name,
                    input_files = file_bins[i],
                    search_archive = select_first([combine_archives.merged_archive]),
                    analysis_schema = search_settings,
                    fasta_1 = fasta_1,
                    fasta_2 = fasta_2,
                    fasta_3 = fasta_3,
                    json_settings = json_settings,
                    cpu = dia_analysis_cpu,
                    ram_gb = dia_analysis_ram_gb,
                    bin_index = i,
                    bin_size_gb = bin_sizes[i],
                    disk_size_multiplier = disk_size_multiplier,
                    n_preemptible = n_preemptible_dia_analysis,
            }
        }

        Array[File] all_sne = select_all(dia_analysis_binned.sne_file)

        # Sum all bin sizes for combine_sne disk allocation
        call sum_floats as sum_sne_sizes {
            input:
                values = bin_sizes,
        }

        # Combine scattered SNE files and generate reports
        call combine_sne {
            input:
                experiment_name = experiment_name,
                sne_files = all_sne,
                analysis_schema = search_settings,
                condition_setup = condition_setup,
                report_schema_1 = report_schema_1,
                report_schema_2 = report_schema_2,
                report_schema_3 = report_schema_3,
                report_schema_4 = report_schema_4,
                cpu = combine_sne_cpu,
                ram_gb = combine_sne_ram_gb,
                total_input_size_gb = sum_sne_sizes.total,
                disk_size_multiplier = disk_size_multiplier,
                enzyme_database = enzyme_database,
                n_preemptible = n_preemptible_combine_sne,
        }
    }

    # Final output: select from single VM or parallel path
    output {
        File spectronaut_output = select_first([combine_sne.spectronaut_output, directDIA_single_vm.spectronaut_output])
    }
}

task list_files {
    input {
        String gcs_path
    }

    command <<<
        set -euo pipefail

        raw_listing="raw_listing.txt"
        cleaned_listing="file_list.txt"

        # Normalize the input path (remove trailing slashes)
        normalized_path=$(echo "~{gcs_path}" | sed 's:/*$::')

        gcloud storage ls "~{gcs_path}" > "${raw_listing}"

        # Remove totals, trim trailing slashes, empty lines, filter out directory itself, unique
        grep -v 'TOTAL:' "${raw_listing}" | \
            sed 's:/*$::' | \
            sed '/^[[:space:]]*$/d' | \
            grep -v "^${normalized_path}$" | \
            sort -u > "${cleaned_listing}"
    >>>

    output {
        File file_list = "file_list.txt"
    }

    runtime {
        docker: "google/cloud-sdk:slim"
        cpu: 2
        memory: "8GB"
        bootDiskSizeGb: 20
        disks: "local-disk 300 HDD"
    }
}

task create_bins {
    input {
        Array[File] htrms_files
        Int num_bins
    }

    command <<<
        python3 <<CODE
import json

# Read HTRMS file paths
file_paths_file = "~{write_lines(htrms_files)}"
with open(file_paths_file) as f:
    files = [line.strip() for line in f if line.strip()]

num_bins = ~{num_bins}

# Validate num_bins
if num_bins < 1:
    raise ValueError(f"num_bins must be at least 1, got {num_bins}")

# Files pre-sorted by list_files task (line 290: sort -u)
# Order preserved through scatter-gather (lines 103-113)
print(f"Processing {len(files)} pre-sorted HTRMS files")

# Calculate actual number of bins
# Cannot have more bins than files
actual_bins = min(num_bins, len(files))

if actual_bins < num_bins:
    print(f"WARNING: Requested {num_bins} bins, but only {len(files)} files available")
    print(f"Setting actual_bins = {actual_bins}")

# Create bins using round-robin distribution
bins = [[] for _ in range(actual_bins)]
for i, file_path in enumerate(files):
    bin_index = i % actual_bins
    bins[bin_index].append(file_path)

# Write bins to JSON
with open("bins.json", "w") as f:
    json.dump(bins, f, indent=2)

# Write calculated_num_vms
with open("calculated_num_vms.txt", "w") as f:
    f.write(str(actual_bins))

# Print summary
print(f"Total files: {len(files)}")
print(f"Calculated number of VMs: {actual_bins}")
for i, bin_files in enumerate(bins):
    print(f"Bin {i}: {len(bin_files)} files")
CODE
    >>>

    output {
        File bins_json = "bins.json"
        Int calculated_num_vms = read_int("calculated_num_vms.txt")
    }

    runtime {
        docker: "python:3.9-slim"
        cpu: 2
        memory: "8GB"
        bootDiskSizeGb: 20
        disks: "local-disk 300 HDD"
    }
}

task sum_floats {
    input {
        Array[Float] values
    }

    command <<<
        python3 <<CODE
import json

# Read values
values_file = "~{write_json(values)}"
with open(values_file) as f:
    values = json.load(f)

# Calculate sum
total = sum(values)

# Write total
with open("total.txt", "w") as f:
    f.write(str(total))

print(f"Sum of {len(values)} values: {total}")
CODE
    >>>

    output {
        Float total = read_float("total.txt")
    }

    runtime {
        docker: "python:3.9-slim"
        cpu: 2
        memory: "8GB"
        bootDiskSizeGb: 20
        disks: "local-disk 300 HDD"
    }
}

task calculate_total_size {
    input {
        Array[File] files
    }

    command <<<
        set -euo pipefail

        # Calculate total size of all files in GB
        echo "Calculating total size of ${~{length(files)}} files..."

        # Use du to get size of each file and sum
        total_size_bytes=0
        while IFS= read -r file_path; do
            if [ -n "${file_path}" ] && [ -f "${file_path}" ]; then
                file_size=$(du -b "${file_path}" | awk '{print $1}')
                total_size_bytes=$((total_size_bytes + file_size))
            fi
        done < ~{write_lines(files)}

        # Convert bytes to GB
        total_size_gb=$(awk "BEGIN {printf \"%.2f\", ${total_size_bytes} / (1024^3)}")
        echo "${total_size_gb}" > total_size_gb.txt
        echo "Total size: ${total_size_gb} GB"
    >>>

    output {
        Float total_size_gb = read_float("total_size_gb.txt")
    }

    runtime {
        docker: "google/cloud-sdk:slim"
        cpu: 2
        memory: "8GB"
        bootDiskSizeGb: 20
        disks: "local-disk 300 HDD"
    }
}

task convert_single_file_htrms {
    input {
        String file_path
        Int disk_size_gb
        File? convert_schema
    }

    command <<<
        set -euo pipefail

        cromwell_root=$(pwd)

        input_dir="${cromwell_root}/work_input"
        mkdir -p "${input_dir}"

        output_dir="${cromwell_root}/out_conversion"
        mkdir -p "${output_dir}"

        tmp_dir="${cromwell_root}/sn_temp"
        mkdir -p "${tmp_dir}"

        # Download single file from GCS
        echo "Downloading file: ~{file_path}"
        gcloud storage cp "~{file_path}" "${input_dir}/"

        # Verify file was downloaded
        file_count=$(find "${input_dir}" -type f | wc -l)
        if [ "${file_count}" -eq 0 ]; then
            echo "ERROR: File download failed" >&2
            exit 1
        fi

        echo "File downloaded successfully."

        # Convert to HTRMS
        echo "Starting HTRMS conversion..."
        spectronaut -convert \
            -i "${input_dir}" \
            -o "${output_dir}" \
            ~{if defined(convert_schema) then "-s " + convert_schema else ""} \
            -setTemp "${tmp_dir}" 2>&1 | tee htrms_conversion.log

        # Move HTRMS file to output
        echo "Moving HTRMS file..."
        htrms_count=$(find "${output_dir}" -type f -name "*.htrms" | wc -l)
        if [ "${htrms_count}" -eq 0 ]; then
            echo "ERROR: No .htrms file produced" >&2
            exit 1
        fi

        find "${output_dir}" -type f -name "*.htrms" -exec mv {} "${cromwell_root}/" \;

        echo "Conversion complete. Generated ${htrms_count} HTRMS file."
    >>>

    output {
        File htrms_file = glob("*.htrms")[0]
    }

    runtime {
        docker: "cameronlian/panoply-spectronaut:v20.3"
        cpu: 8
        memory: "16GB"
        bootDiskSizeGb: 128
        disks: "local-disk ~{disk_size_gb} SSD"
        preemptible: 0
    }
}

task directDIA_single_vm {
    input {
        String experiment_name
        Array[File] input_files
        Float total_size_gb
        Int disk_size_multiplier
        File fasta_1
        Int cpu
        Int ram_gb
        File? analysis_schema
        File? fasta_2
        File? fasta_3
        File? enzyme_database
        File? json_settings
        File? condition_setup
        File? report_schema_1
        File? report_schema_2
        File? report_schema_3
        File? report_schema_4
        Int n_preemptible = 0
    }

    command <<<
        set -euo pipefail

        cromwell_root=$(pwd)

        input_dir="${cromwell_root}/work_input"
        mkdir -p "${input_dir}"

        output_dir="${cromwell_root}/out_spectronaut"
        mkdir -p "${output_dir}"

        tmp_dir="${cromwell_root}/sn_temp"
        mkdir -p "${tmp_dir}"

        output_zip="${cromwell_root}/spectronaut_output.zip"

        # Copy all HTRMS files to input directory
        echo "Copying HTRMS files to input directory..."
        while IFS= read -r htrms_file; do
            if [ -n "${htrms_file}" ] && [ -f "${htrms_file}" ]; then
                cp "${htrms_file}" "${input_dir}/"
            fi
        done < ~{write_lines(input_files)}

        # Verify files were copied
        file_count=$(find "${input_dir}" -type f -name "*.htrms" | wc -l)
        echo "Copied ${file_count} HTRMS files to input directory"

        # Import enzyme database if provided
        if [ ~{defined(enzyme_database)} = true ]; then
            echo "Importing enzyme database..."
            dotnet /usr/lib/spectronaut/SpectronautCMD.dll --importEnzymeDB "~{enzyme_database}"
        fi

        # Run DirectDIA search
        echo "Starting DirectDIA search..."
        spectronaut direct \
            ~{if defined(analysis_schema) then "-s " + analysis_schema else ""} \
            ~{if defined(condition_setup) then "-con " + condition_setup else ""} \
            -fasta "~{fasta_1}" \
            ~{if defined(fasta_2) then "-fasta " + fasta_2 else ""} \
            ~{if defined(fasta_3) then "-fasta " + fasta_3 else ""} \
            ~{if defined(json_settings) then "-j " + json_settings else ""} \
            ~{if defined(report_schema_1) then "-rs " + report_schema_1 else ""} \
            ~{if defined(report_schema_2) then "-rs " + report_schema_2 else ""} \
            ~{if defined(report_schema_3) then "-rs " + report_schema_3 else ""} \
            ~{if defined(report_schema_4) then "-rs " + report_schema_4 else ""} \
            -n "~{experiment_name}" \
            -o "${output_dir}" \
            -d "${input_dir}" \
            -setTemp "${tmp_dir}" 2>&1 | tee spectronaut_single_vm.log

        # Create output archive
        echo "Creating output archive..."
        zip -r "${output_zip}" "${output_dir}" -x \*.zip

        if [ ! -f "${output_zip}" ]; then
            echo "ERROR: Failed to create output zip file" >&2
            exit 1
        fi

        echo "DirectDIA search complete."

        # Memory usage reporting
        echo "=== Memory Usage Report ==="
        # Cgroup V2 (modern)
        if [ -f /sys/fs/cgroup/memory.peak ]; then
            max_mem_bytes=$(cat /sys/fs/cgroup/memory.peak)
            limit_bytes=$(cat /sys/fs/cgroup/memory.max)
        # Cgroup V1 (legacy)
        elif [ -f /sys/fs/cgroup/memory/memory.max_usage_in_bytes ]; then
            max_mem_bytes=$(cat /sys/fs/cgroup/memory/memory.max_usage_in_bytes)
            limit_bytes=$(cat /sys/fs/cgroup/memory/memory.limit_in_bytes)
        else
            max_mem_bytes=0
            limit_bytes=0
        fi

        if [ "$max_mem_bytes" -gt 0 ]; then
            max_mem_gb=$(awk -v val="$max_mem_bytes" 'BEGIN { print val / (1024^3) }')
            echo "Actual Peak RAM: ${max_mem_gb} GB"

            if [ "$limit_bytes" -gt 0 ] && [ "$limit_bytes" != "max" ]; then
                limit_gb=$(awk -v val="$limit_bytes" 'BEGIN { print val / (1024^3) }')
                usage_percent=$(awk -v max="$max_mem_bytes" -v lim="$limit_bytes" 'BEGIN { print (max / lim) * 100 }')
                echo "RAM Limit: ${limit_gb} GB"
                echo "RAM Usage: ${usage_percent}%"
            fi
        fi
        echo "==========================="
    >>>

    output {
        File spectronaut_output = "spectronaut_output.zip"
    }

    runtime {
        docker: "cameronlian/panoply-spectronaut:v20.3"
        cpu: cpu
        memory: "~{ram_gb}GB"
        bootDiskSizeGb: 128
        disks: "local-disk ~{ceil(total_size_gb * disk_size_multiplier)} SSD"
        preemptible: n_preemptible
    }
}

task directDIA_search_binned {
    input {
        Array[File] input_files
        Float bin_size_gb
        Int disk_size_multiplier
        File fasta_1
        Int cpu
        Int ram_gb
        Int bin_index
        File? analysis_schema
        File? fasta_2
        File? fasta_3
        File? enzyme_database
        Int n_preemptible = 0
    }

    command <<<
        set -euo pipefail

        cromwell_root=$(pwd)

        input_dir="${cromwell_root}/work_input"
        mkdir -p "${input_dir}"

        output_dir="${cromwell_root}/out_archive"
        mkdir -p "${output_dir}"

        tmp_dir="${cromwell_root}/sn_temp"
        mkdir -p "${tmp_dir}"

        # Copy all HTRMS files to input directory
        echo "Copying HTRMS files..."
        while IFS= read -r htrms_file; do
            if [ -n "${htrms_file}" ]; then
                cp "${htrms_file}" "${input_dir}/"
            fi
        done < ~{write_lines(input_files)}

        # Import enzyme database if provided
        if [ ~{defined(enzyme_database)} = true ]; then
            echo "Importing enzyme database..."
            dotnet /usr/lib/spectronaut/SpectronautCMD.dll --importEnzymeDB "~{enzyme_database}"
        fi

        spectronaut direct \
            -d "${input_dir}" \
            -fasta "~{fasta_1}" \
            ~{if defined(fasta_2) then "-fasta " + fasta_2 else ""} \
            ~{if defined(fasta_3) then "-fasta " + fasta_3 else ""} \
            ~{if defined(analysis_schema) then "-s " + analysis_schema else ""} \
            -o "${output_dir}" \
            -setTemp "${tmp_dir}" 2>&1 | tee archive_generation.log

        # Rename the single .psar file with bin_index to ensure uniqueness across shards
        echo "Moving and renaming search archive with bin_index ~{bin_index}..."
        psar_file=$(find "${output_dir}" -type f -name "*.psar" | head -n 1)

        if [ -z "${psar_file}" ] || [ ! -f "${psar_file}" ]; then
            echo "ERROR: No .psar file produced" >&2
            exit 1
        fi

        # Rename to predictable name with bin_index
        output_name="search_archive_bin_~{bin_index}.psar"
        mv "${psar_file}" "${cromwell_root}/${output_name}"

        echo "Archive generation complete. Generated ${output_name}"

        # Memory usage reporting
        echo "=== Memory Usage Report ==="
        # Cgroup V2 (modern)
        if [ -f /sys/fs/cgroup/memory.peak ]; then
            max_mem_bytes=$(cat /sys/fs/cgroup/memory.peak)
            limit_bytes=$(cat /sys/fs/cgroup/memory.max)
        # Cgroup V1 (legacy)
        elif [ -f /sys/fs/cgroup/memory/memory.max_usage_in_bytes ]; then
            max_mem_bytes=$(cat /sys/fs/cgroup/memory/memory.max_usage_in_bytes)
            limit_bytes=$(cat /sys/fs/cgroup/memory/memory.limit_in_bytes)
        else
            max_mem_bytes=0
            limit_bytes=0
        fi

        if [ "$max_mem_bytes" -gt 0 ]; then
            max_mem_gb=$(awk -v val="$max_mem_bytes" 'BEGIN { print val / (1024^3) }')
            echo "Actual Peak RAM: ${max_mem_gb} GB"
            
            if [ "$limit_bytes" -gt 0 ] && [ "$limit_bytes" != "max" ]; then
                limit_gb=$(awk -v val="$limit_bytes" 'BEGIN { print val / (1024^3) }')
                usage_percent=$(awk -v max="$max_mem_bytes" -v lim="$limit_bytes" 'BEGIN { print (max / lim) * 100 }')
                echo "RAM Limit: ${limit_gb} GB"
                echo "RAM Usage: ${usage_percent}%"
            fi
        fi
        echo "==========================="
    >>>

    output {
        File search_archive = "search_archive_bin_~{bin_index}.psar"
    }

    runtime {
        docker: "cameronlian/panoply-spectronaut:v20.3"
        cpu: cpu
        memory: "~{ram_gb}GB"
        bootDiskSizeGb: 128
        disks: "local-disk ~{ceil(bin_size_gb * disk_size_multiplier)} SSD"
        preemptible: n_preemptible
    }
}

task combine_archives {
    input {
        Array[File] input_archives
        Float total_input_size_gb
        Int disk_size_multiplier
        Int cpu
        Int ram_gb
        File? enzyme_database
        Int n_preemptible = 0
    }

    command <<<
        set -euo pipefail

        cromwell_root=$(pwd)
        merged_library="merged_library.kit"

        work_archives="${cromwell_root}/work_archives"
        mkdir -p "${work_archives}"

        echo "Copying archives for merging..."
        while IFS= read -r archive; do
            if [ -n "${archive}" ]; then
                cp "${archive}" "${work_archives}/"
            fi
        done < ~{write_lines(input_archives)}

        # Import enzyme database if provided
        if [ ~{defined(enzyme_database)} = true ]; then
            echo "Importing enzyme database..."
            dotnet /usr/lib/spectronaut/SpectronautCMD.dll --importEnzymeDB "~{enzyme_database}"
        fi

        spectronaut lg -se Pulsar \
            -sad "${work_archives}" \
            -k "${cromwell_root}/${merged_library}" \
            -o "${cromwell_root}" 2>&1 | tee merge_archives.log

        if [ ! -f "${cromwell_root}/${merged_library}" ]; then
            echo "ERROR: Merged archive file not found" >&2
            exit 1
        fi

        echo "Archive merging complete."

        # Memory usage reporting
        echo "=== Memory Usage Report ==="
        # Cgroup V2 (modern)
        if [ -f /sys/fs/cgroup/memory.peak ]; then
            max_mem_bytes=$(cat /sys/fs/cgroup/memory.peak)
            limit_bytes=$(cat /sys/fs/cgroup/memory.max)
        # Cgroup V1 (legacy)
        elif [ -f /sys/fs/cgroup/memory/memory.max_usage_in_bytes ]; then
            max_mem_bytes=$(cat /sys/fs/cgroup/memory/memory.max_usage_in_bytes)
            limit_bytes=$(cat /sys/fs/cgroup/memory/memory.limit_in_bytes)
        else
            max_mem_bytes=0
            limit_bytes=0
        fi

        if [ "$max_mem_bytes" -gt 0 ]; then
            max_mem_gb=$(awk -v val="$max_mem_bytes" 'BEGIN { print val / (1024^3) }')
            echo "Actual Peak RAM: ${max_mem_gb} GB"
            
            if [ "$limit_bytes" -gt 0 ] && [ "$limit_bytes" != "max" ]; then
                limit_gb=$(awk -v val="$limit_bytes" 'BEGIN { print val / (1024^3) }')
                usage_percent=$(awk -v max="$max_mem_bytes" -v lim="$limit_bytes" 'BEGIN { print (max / lim) * 100 }')
                echo "RAM Limit: ${limit_gb} GB"
                echo "RAM Usage: ${usage_percent}%"
            fi
        fi
        echo "==========================="
    >>>

    output {
        File merged_archive = "merged_library.kit"
    }

    runtime {
        docker: "cameronlian/panoply-spectronaut:v20.3"
        cpu: cpu
        memory: "~{ram_gb}GB"
        bootDiskSizeGb: 128
        disks: "local-disk ~{ceil(total_input_size_gb * disk_size_multiplier)} HDD"
        preemptible: n_preemptible
    }
}

task dia_analysis_binned {
    input {
        Array[File] input_files
        File search_archive
        Float bin_size_gb
        Int disk_size_multiplier
        File fasta_1
        Int cpu
        Int ram_gb
        Int bin_index
        String experiment_name
        File? enzyme_database
        File? analysis_schema
        File? fasta_2
        File? fasta_3
        File? json_settings
        Int n_preemptible = 0
    }

    command <<<
        set -euo pipefail

        cromwell_root=$(pwd)

        input_dir="${cromwell_root}/work_input"
        mkdir -p "${input_dir}"

        output_dir="${cromwell_root}/out_dia"
        mkdir -p "${output_dir}"

        tmp_dir="${cromwell_root}/work_dia_temp"
        mkdir -p "${tmp_dir}"

        # Copy all HTRMS files to input directory
        echo "Copying HTRMS files..."
        while IFS= read -r htrms_file; do
            if [ -n "${htrms_file}" ] && [ -f "${htrms_file}" ]; then
                cp "${htrms_file}" "${input_dir}/"
            fi
        done < ~{write_lines(input_files)}

                # Import enzyme database if provided
        if [ ~{defined(enzyme_database)} = true ]; then
            echo "Importing enzyme database..."
            dotnet /usr/lib/spectronaut/SpectronautCMD.dll --importEnzymeDB "~{enzyme_database}"
        fi

        spectronaut diaanalysis \
            ~{if defined(analysis_schema) then "-s " + analysis_schema else ""} \
            -fasta "~{fasta_1}" \
            ~{if defined(fasta_2) then "-fasta " + fasta_2 else ""} \
            ~{if defined(fasta_3) then "-fasta " + fasta_3 else ""} \
            ~{if defined(json_settings) then "-j " + json_settings else ""} \
            -n "~{experiment_name}_bin_~{bin_index}" \
            -o "${output_dir}" \
            -d "${input_dir}" \
            -a "~{search_archive}" \
            -setTemp "${tmp_dir}" 2>&1 | tee dia_analysis.log

        # Move the single .sne file to cromwell root
        echo "Moving SNE file from bin ~{bin_index}..."
        sne_file=$(find "${output_dir}" -type f -name "*.sne" | head -n 1)

        if [ -z "${sne_file}" ] || [ ! -f "${sne_file}" ]; then
            echo "ERROR: No .sne file found" >&2
            exit 1
        fi

        # Move to cromwell root (file is already named with experiment_name and bin_index)
        mv "${sne_file}" "${cromwell_root}/"

        echo "DIA analysis complete. Generated SNE file: $(basename "${sne_file}")"

        # Memory usage reporting
        echo "=== Memory Usage Report ==="
        # Cgroup V2 (modern)
        if [ -f /sys/fs/cgroup/memory.peak ]; then
            max_mem_bytes=$(cat /sys/fs/cgroup/memory.peak)
            limit_bytes=$(cat /sys/fs/cgroup/memory.max)
        # Cgroup V1 (legacy)
        elif [ -f /sys/fs/cgroup/memory/memory.max_usage_in_bytes ]; then
            max_mem_bytes=$(cat /sys/fs/cgroup/memory/memory.max_usage_in_bytes)
            limit_bytes=$(cat /sys/fs/cgroup/memory/memory.limit_in_bytes)
        else
            max_mem_bytes=0
            limit_bytes=0
        fi

        if [ "$max_mem_bytes" -gt 0 ]; then
            max_mem_gb=$(awk -v val="$max_mem_bytes" 'BEGIN { print val / (1024^3) }')
            echo "Actual Peak RAM: ${max_mem_gb} GB"
            
            if [ "$limit_bytes" -gt 0 ] && [ "$limit_bytes" != "max" ]; then
                limit_gb=$(awk -v val="$limit_bytes" 'BEGIN { print val / (1024^3) }')
                usage_percent=$(awk -v max="$max_mem_bytes" -v lim="$limit_bytes" 'BEGIN { print (max / lim) * 100 }')
                echo "RAM Limit: ${limit_gb} GB"
                echo "RAM Usage: ${usage_percent}%"
            fi
        fi
        echo "==========================="
    >>>

    output {
        File sne_file = "~{experiment_name}_bin_~{bin_index}.sne"
    }

    runtime {
        docker: "cameronlian/panoply-spectronaut:v20.3"
        cpu: cpu
        memory: "~{ram_gb}GB"
        bootDiskSizeGb: 128
        disks: "local-disk ~{ceil(bin_size_gb * disk_size_multiplier)} SSD"
        preemptible: n_preemptible
    }
}

task combine_sne {
    input {
        Array[File] sne_files
        String experiment_name
        Float total_input_size_gb
        Int disk_size_multiplier
        Int ram_gb
        Int cpu
        File? condition_setup
        File? report_schema_1
        File? report_schema_2
        File? report_schema_3
        File? report_schema_4
        File? analysis_schema
        File? enzyme_database
        Int n_preemptible = 0
    }

    command <<<
        set -euo pipefail

        cromwell_root=$(pwd)

        output_dir="${cromwell_root}/out_combine"
        output_zip="${cromwell_root}/spectronaut_output.zip"

        mkdir -p "${output_dir}"

        sne_dir="${cromwell_root}/work_snes"
        mkdir -p "${sne_dir}"

        echo "Copying SNE files for merging..."
        while IFS= read -r sne_file; do
            if [ -n "${sne_file}" ]; then
                cp "${sne_file}" "${sne_dir}/"
            fi
        done < ~{write_lines(sne_files)}

        # Import enzyme database if provided
        if [ ~{defined(enzyme_database)} = true ]; then
            echo "Importing enzyme database..."
            dotnet /usr/lib/spectronaut/SpectronautCMD.dll --importEnzymeDB "~{enzyme_database}"
        fi

        spectronaut manageSNE --merge \
            -n "~{experiment_name}_merged" \
            -o "${output_dir}" \
            -d "${sne_dir}" \
            ~{if defined(condition_setup) then "-con " + condition_setup else ""} \
            ~{if defined(analysis_schema) then "-s " + analysis_schema else ""} \
            ~{if defined(report_schema_1) then "-rs " + report_schema_1 else ""} \
            ~{if defined(report_schema_2) then "-rs " + report_schema_2 else ""} \
            ~{if defined(report_schema_3) then "-rs " + report_schema_3 else ""} \
            ~{if defined(report_schema_4) then "-rs " + report_schema_4 else ""} 2>&1 | tee spectronaut_combine.log

        echo "Creating output archive..."
        zip -r "${output_zip}" "${output_dir}" -x \*.zip

        if [ ! -f "${output_zip}" ]; then
            echo "ERROR: Failed to create output zip file" >&2
            exit 1
        fi

        echo "SNE merging complete."

        # Memory usage reporting
        echo "=== Memory Usage Report ==="
        # Cgroup V2 (modern)
        if [ -f /sys/fs/cgroup/memory.peak ]; then
            max_mem_bytes=$(cat /sys/fs/cgroup/memory.peak)
            limit_bytes=$(cat /sys/fs/cgroup/memory.max)
        # Cgroup V1 (legacy)
        elif [ -f /sys/fs/cgroup/memory/memory.max_usage_in_bytes ]; then
            max_mem_bytes=$(cat /sys/fs/cgroup/memory/memory.max_usage_in_bytes)
            limit_bytes=$(cat /sys/fs/cgroup/memory/memory.limit_in_bytes)
        else
            max_mem_bytes=0
            limit_bytes=0
        fi

        if [ "$max_mem_bytes" -gt 0 ]; then
            max_mem_gb=$(awk -v val="$max_mem_bytes" 'BEGIN { print val / (1024^3) }')
            echo "Actual Peak RAM: ${max_mem_gb} GB"
            
            if [ "$limit_bytes" -gt 0 ] && [ "$limit_bytes" != "max" ]; then
                limit_gb=$(awk -v val="$limit_bytes" 'BEGIN { print val / (1024^3) }')
                usage_percent=$(awk -v max="$max_mem_bytes" -v lim="$limit_bytes" 'BEGIN { print (max / lim) * 100 }')
                echo "RAM Limit: ${limit_gb} GB"
                echo "RAM Usage: ${usage_percent}%"
            fi
        fi
        echo "==========================="
    >>>

    output {
        File spectronaut_output = "spectronaut_output.zip"
    }

    runtime {
        docker: "cameronlian/panoply-spectronaut:v20.3"
        cpu: cpu
        memory: "~{ram_gb}GB"
        bootDiskSizeGb: 128
        disks: "local-disk ~{ceil(total_input_size_gb * disk_size_multiplier)} HDD"
        preemptible: n_preemptible
    }
}

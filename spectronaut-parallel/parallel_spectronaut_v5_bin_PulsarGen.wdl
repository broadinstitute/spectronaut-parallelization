version development

# Universal HTRMS Conversion with Conditional Search Modes
# Phase I: Each raw file converted to HTRMS in its own VM
# Phase II: HTRMS files binned across n VMs
# Phase III: Conditional execution - Single VM (num_vms=1) or Parallel (num_vms>1)
workflow parallel_spectronaut {
    input {
        File fasta_1  # Primary FASTA database file (required)
        # ============================================================================
        # Required Workflow Parameters
        # ============================================================================
        String experiment_name  # Experiment identifier
        String file_directory  # GCS path to raw input files

        # ============================================================================
        # Optional Database Files
        # ============================================================================
        File? fasta_2  # Additional FASTA database file
        File? fasta_3  # Additional FASTA database file
        File? enzyme_database  # Custom enzyme database

        # ============================================================================
        # Analysis Settings & Schemas
        # ============================================================================
        File? convert_schema  # Schema for HTRMS conversion
        File? search_settings  # Settings for directDIA search and DIA analysis
        File? json_settings  # JSON settings for Spectronaut
        File? condition_setup  # Experimental condition setup

        # ============================================================================
        # Report Schemas
        # ============================================================================
        File? report_schema_1  # Report schema 1
        File? report_schema_2  # Report schema 2
        File? report_schema_3  # Report schema 3
        File? report_schema_4  # Report schema 4

        # ============================================================================
        # Workflow Configuration
        # ============================================================================
        String experiment_type = "proteome"  # Experiment type: "proteome" or "ptm" (affects resource presets)
        Boolean do_conversion = true  # Enable HTRMS conversion (always true in current version)
        Int num_vms = 1  # Number of VMs for parallel processing (1 = single VM, >1 = parallel)

        # ============================================================================
        # Resource Configuration
        # ============================================================================
        Int disk_size_multiplier = 3  # Multiplier for dynamic disk size calculation
        Int htrms_conversion_disk_gb = 100  # Fixed disk size per VM for HTRMS conversion

        # Preemptible instance settings (0 = non-preemptible, >0 = number of preemptible attempts)
        Int n_preemptible_htrms_conversion = 2  # HTRMS conversion preemptible attempts
        Int n_preemptible_directDIA_search = 0  # directDIA search preemptible attempts
        Int n_preemptible_combine_archives = 0  # Archive combining preemptible attempts
        Int n_preemptible_dia_analysis = 0  # DIA analysis preemptible attempts
        Int n_preemptible_combine_sne = 0  # SNE combining preemptible attempts
    }

    # Compute preset configurations based on experiment_type
    Map[String, Int] directDIA_search_cpu_presets = {
        "proteome": 32,
        "ptm": 48,
    }
    Map[String, Int] directDIA_search_ram_gb_presets = {
        "proteome": 80,
        "ptm": 120,
    }

    Map[String, Int] combine_archives_cpu_presets = {
        "proteome": 16,
        "ptm": 16,
    }
    Map[String, Int] combine_archives_ram_gb_presets = {
        "proteome": 50,
        "ptm": 80,
    }

    Map[String, Int] dia_analysis_cpu_presets = {
        "proteome": 32,
        "ptm": 48,
    }
    Map[String, Int] dia_analysis_ram_gb_presets = {
        "proteome": 80,
        "ptm": 120,
    }

    Map[String, Int] combine_sne_cpu_presets = {
        "proteome": 16,
        "ptm": 16,
    }
    Map[String, Int] combine_sne_ram_gb_presets = {
        "proteome": 40,
        "ptm": 80,
    }

    Map[String, String] directDIA_search_machine_presets = {
        "proteome": "n2d-highcpu-96",
        "ptm": "n2d-highcpu-128",
    }

    Map[String, String] combine_archives_machine_presets = {
        "proteome": "n2d-highmem-16",
        "ptm": "n2d-highmem-16",
    }

    Map[String, String] dia_analysis_machine_presets = {
        "proteome": "n2d-highcpu-48",
        "ptm": "n2d-highcpu-64",
    }

    Map[String, String] combine_sne_machine_presets = {
        "proteome": "n2d-highmem-16",
        "ptm": "n2d-highmem-16",
    }

    # Validate experiment_type and fallback to "proteome" if invalid
    String validated_experiment_type = if (experiment_type == "proteome" || experiment_type
        == "ptm") then experiment_type else "proteome"

    # Look up values based on validated_experiment_type
    Int directDIA_search_cpu = directDIA_search_cpu_presets[validated_experiment_type]
    Int directDIA_search_ram_gb = directDIA_search_ram_gb_presets[
        validated_experiment_type]

    Int combine_archives_cpu = combine_archives_cpu_presets[validated_experiment_type]
    Int combine_archives_ram_gb = combine_archives_ram_gb_presets[
        validated_experiment_type]

    Int dia_analysis_cpu = dia_analysis_cpu_presets[validated_experiment_type]
    Int dia_analysis_ram_gb = dia_analysis_ram_gb_presets[validated_experiment_type]

    Int combine_sne_cpu = combine_sne_cpu_presets[validated_experiment_type]
    Int combine_sne_ram_gb = combine_sne_ram_gb_presets[validated_experiment_type]

    String directDIA_search_machine = directDIA_search_machine_presets[validated_experiment_type]
    String combine_archives_machine = combine_archives_machine_presets[validated_experiment_type]
    String dia_analysis_machine = dia_analysis_machine_presets[validated_experiment_type]
    String combine_sne_machine = combine_sne_machine_presets[validated_experiment_type]

    # ============================================================================
    # PHASE I: Discovery and Optional HTRMS Conversion (1 file per VM)
    # ============================================================================

    # List all raw files in the input directory
    call list_files { input:
        gcs_path = file_directory,
    }

    Array[String] raw_file_paths = read_lines(list_files.file_list)

    # ============================================================================
    # CONDITIONAL PATH: do_conversion=true vs false
    # ============================================================================

    if (do_conversion && num_vms == 1) {
        # CONVERSION PATH: Scatter convert and calculate sizes per file (only in single VM mode)
        scatter (raw_file_path in raw_file_paths) {
            call convert_single_file_htrms { input:
                file_path = raw_file_path,
                disk_size_gb = htrms_conversion_disk_gb,
                convert_schema = convert_schema,
                n_preemptible = n_preemptible_htrms_conversion,
            }
        }

        # Gather converted files and their sizes
        Array[File] converted_files = convert_single_file_htrms.htrms_file
        Array[Float] converted_file_sizes = convert_single_file_htrms.htrms_size_gb

        # Sum all individual converted file sizes
        call sum_floats as sum_converted_sizes { input:
            values = converted_file_sizes,
        }
    }

    if (!do_conversion || num_vms > 1) {
        # NO CONVERSION PATH: Calculate directory size via gcloud when skipping conversion
        call calculate_directory_size_gcs { input:
            gcs_directory_path = file_directory,
        }
    }

    # Select appropriate file array and total size based on path
    Array[File] all_input_files = select_first([
        converted_files,
        read_lines(list_files.file_list),
    ])
    Float total_input_size_gb = select_first([
        sum_converted_sizes.total,
        calculate_directory_size_gcs.total_size_gb,
    ])

    # ============================================================================
    # PHASE II: Intelligent Binning
    # ============================================================================

    # Bin input files with sorting and validation
    call create_bins { input:
        file_paths = read_lines(write_lines(all_input_files)),
        num_bins = num_vms,
    }

    Array[Array[File]] file_bins = read_json(create_bins.bins_json)
    Int calculated_num_vms = create_bins.calculated_num_vms

    # ============================================================================
    # PHASE III: Conditional Execution - Single VM vs Parallel
    # ============================================================================

    # BRANCH A: Single VM Mode (num_vms == 1)
    if (calculated_num_vms == 1) {
        # Run classic directDIA on all files in one VM
        call directDIA_single_vm { input:
            experiment_name = experiment_name,
            input_files = all_input_files,
            total_size_gb = total_input_size_gb,
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
            predefined_machine_type = directDIA_search_machine,
            allocated_disk_gb = ceil(total_input_size_gb * disk_size_multiplier),
        }
    }

    # BRANCH B: Parallel Mode (num_vms > 1)
    if (calculated_num_vms > 1) {

        # Calculate approximate size per VM for disk allocation
        Float bin_size_per_vm = total_input_size_gb / calculated_num_vms + 25

        # Scatter: Generate search archives for each bin
        scatter (i in range(length(file_bins))) {

            # directDIA search for search archive generation
            call directDIA_search_binned { input:
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
                predefined_machine_type = directDIA_search_machine,
                bin_size_gb = bin_size_per_vm,
                disk_size_multiplier = disk_size_multiplier,
                allocated_disk_gb = ceil(bin_size_per_vm * disk_size_multiplier),
            }
        }

        # Collect all archives from all bins (each bin produces multiple archives)
        Array[Array[File]] all_archives_nested = directDIA_search_binned.search_archives
        Array[File] all_archives = flatten(all_archives_nested)

        # Combine scattered search archives into one
        # Uses total_input_size_gb for disk estimation instead of summing bin sizes
        call combine_archives { input:
            input_archives = all_archives,
            total_input_size_gb = total_input_size_gb,
            disk_size_multiplier = disk_size_multiplier,
            cpu = combine_archives_cpu,
            ram_gb = combine_archives_ram_gb,
            enzyme_database = enzyme_database,
            n_preemptible = n_preemptible_combine_archives,
            predefined_machine_type = combine_archives_machine,
            allocated_disk_gb = ceil(total_input_size_gb * disk_size_multiplier),
        }

        # Scatter: DIA analysis for each bin against the merged library
        scatter (i in range(length(file_bins))) {
            call dia_analysis_binned { input:
                experiment_name = experiment_name,
                input_files = file_bins[i],
                search_archive = combine_archives.merged_archive,
                analysis_schema = search_settings,
                fasta_1 = fasta_1,
                fasta_2 = fasta_2,
                fasta_3 = fasta_3,
                json_settings = json_settings,
                cpu = dia_analysis_cpu,
                ram_gb = dia_analysis_ram_gb,
                bin_index = i,
                bin_size_gb = bin_size_per_vm,
                disk_size_multiplier = disk_size_multiplier,
                n_preemptible = n_preemptible_dia_analysis,
                predefined_machine_type = dia_analysis_machine,
                allocated_disk_gb = ceil(bin_size_per_vm * disk_size_multiplier),
            }
        }

        Array[Array[File]] all_sne_nested = dia_analysis_binned.sne_files
        Array[File] all_sne = flatten(all_sne_nested)

        # Combine scattered SNE files and generate reports
        call combine_sne { input:
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
            total_input_size_gb = total_input_size_gb,
            disk_size_multiplier = disk_size_multiplier,
            enzyme_database = enzyme_database,
            n_preemptible = n_preemptible_combine_sne,
            predefined_machine_type = combine_sne_machine,
            allocated_disk_gb = ceil(total_input_size_gb * disk_size_multiplier),
        }
    }

    # Final output: select from single VM or parallel path
    output {
        File spectronaut_output = select_first([
            combine_sne.spectronaut_output,
            directDIA_single_vm.spectronaut_output,
        ])
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
        disks: "local-disk 50 HDD"
        cpuPlatform: "AMD Rome"
    }
}

task create_bins {
    input {
        Array[String] file_paths
        Int num_bins
    }

    command <<<
                python3 <<CODE
        import json
        import os

        cromwell_root = os.getcwd()

        # Read HTRMS file paths
        file_paths_file = "~{write_lines(file_paths)}"
        with open(file_paths_file) as f:
            files = [line.strip() for line in f if line.strip()]

        num_bins = ~{num_bins}

        # Cap num_bins at 50 if user requested more than 100
        if num_bins > 100:
            print(f"WARNING: Requested {num_bins} VMs, which exceeds maximum of 100")
            print(f"Reducing num_bins to 50 for resource optimization")
            num_bins = 50

        # Validate num_bins
        if num_bins < 1:
            raise ValueError(f"num_bins must be at least 1, got {num_bins}")

        # Files pre-sorted by list_files task (line 290: sort -u)
        # Order preserved through scatter-gather (lines 103-113)
        print(f"Processing {len(files)} pre-sorted input files")

        # Calculate actual number of bins
        # Cannot have more bins than files
        actual_bins = min(num_bins, len(files))

        if actual_bins < num_bins:
            print(f"WARNING: Requested {num_bins} bins, but only {len(files)} files available")
            print(f"Setting actual_bins = {actual_bins}")

        # Safety check: Cap actual_bins at 50 if it exceeds 100
        if actual_bins > 100:
            print(f"WARNING: Calculated {actual_bins} bins, which exceeds maximum of 100")
            print(f"Reducing actual_bins to 50 for resource optimization")
            actual_bins = 50

        # Efficiency check: Force parallelization if user requested 1 VM for > 36 files
        if num_bins == 1 and len(files) > 36:
            print(f"WARNING: Requested 1 VM for {len(files)} files (> 36)")
            print(f"Setting actual_bins to 10 for efficient parallel analysis")
            actual_bins = 10

        # Create bins using round-robin distribution
        bins = [[] for _ in range(actual_bins)]
        for i, file_path in enumerate(files):
            bin_index = i % actual_bins
            bins[bin_index].append(file_path)

        # Write bins to JSON
        with open(f"{cromwell_root}/bins.json", "w") as f:
            json.dump(bins, f, indent=2)

        # Write calculated_num_vms
        with open(f"{cromwell_root}/calculated_num_vms.txt", "w") as f:
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
        disks: "local-disk 50 HDD"
        cpuPlatform: "AMD Rome"
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
        disks: "local-disk 50 HDD"
        cpuPlatform: "AMD Rome"
    }
}

task calculate_directory_size_gcs {
    input {
        String gcs_directory_path
    }

    command <<<
        set -euo pipefail

        # Normalize path (remove trailing slash)
        normalized_path=$(echo "~{gcs_directory_path}" | sed 's:/*$::')

        echo "Calculating size of GCS directory: ${normalized_path}"

        # Use gcloud storage du to get total size
        # Output format: <bytes> <path>
        size_bytes=$(gcloud storage du -s "${normalized_path}" | awk '{print $1}')

        if [ -z "${size_bytes}" ] || [ "${size_bytes}" -eq 0 ]; then
            echo "WARNING: Directory size is 0 or could not be determined" >&2
            # Set minimum size to avoid zero disk allocation
            size_bytes=1073741824  # 1 GB minimum
        fi

        # Convert bytes to GB
        size_gb=$(awk "BEGIN {printf \"%.2f\", ${size_bytes} / (1024^3)}")
        echo "${size_gb}" > total_size_gb.txt

        echo "Total directory size: ${size_gb} GB (${size_bytes} bytes)"
    >>>

    output {
        Float total_size_gb = read_float("total_size_gb.txt")
    }

    runtime {
        docker: "google/cloud-sdk:slim"
        cpu: 2
        memory: "8GB"
        bootDiskSizeGb: 20
        disks: "local-disk 50 HDD"
        cpuPlatform: "AMD Rome"
    }
}

task convert_single_file_htrms {
    input {
        String file_path
        Int disk_size_gb
        Int n_preemptible
        File? convert_schema
    }

    command <<<
        set -euo pipefail

        cromwell_root=$(pwd)

        # Resource Monitoring Initialization
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

        input_dir="${cromwell_root}/work_input"
        mkdir -p "${input_dir}"

        output_dir="${cromwell_root}/out_conversion"
        mkdir -p "${output_dir}"

        tmp_dir="${cromwell_root}/sn_temp"
        mkdir -p "${tmp_dir}"

        # Download single file from GCS
        echo "Downloading file: ~{file_path}"
        gcloud storage cp -r "~{file_path}" "${input_dir}/"

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

        # Calculate HTRMS file size
        echo "Calculating HTRMS file size..."
        htrms_file_path=$(find "${cromwell_root}" -type f -name "*.htrms")
        if [ -f "${htrms_file_path}" ]; then
            size_bytes=$(stat -c%s "${htrms_file_path}" 2>/dev/null || stat -f%z "${htrms_file_path}")
            size_gb=$(awk "BEGIN {printf \"%.2f\", ${size_bytes} / (1024^3)}")
            echo "${size_gb}" > "${cromwell_root}/htrms_size_gb.txt"
            echo "HTRMS file size: ${size_gb} GB"
        else
            echo "ERROR: HTRMS file not found for size calculation" >&2
            exit 1
        fi

        # ============================================================================
        # Resource Usage Report
        # ============================================================================
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

        if [ "$cpu_start_ns" -gt 0 ] && [ "$cpu_end_ns" -gt "$cpu_start_ns" ] && [ "$wall_elapsed_ns" -gt 0 ]; then
            cpu_used_ns=$((cpu_end_ns - cpu_start_ns))
            cpu_utilization_total=$(awk -v cpu="$cpu_used_ns" -v wall="$wall_elapsed_ns" \
                'BEGIN { printf "%.2f", (cpu / wall) * 100 }')
            allocated_cpus=6
            cpu_utilization_per_core=$(awk -v total="$cpu_utilization_total" -v cpus="$allocated_cpus" \
                'BEGIN { printf "%.2f", total / cpus }')

            echo "Allocated CPUs: ${allocated_cpus}"
            echo "Total CPU Utilization: ${cpu_utilization_total}% (across all cores)"
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

            if [ "$limit_bytes" -gt 0 ] && [ "$limit_bytes" != "max" ]; then
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

        allocated_disk_gb=~{disk_size_gb}
        echo "Disk Size Assigned: ${allocated_disk_gb} GB"

        if command -v df >/dev/null 2>&1; then
            disk_used_output=$(df -BG "${cromwell_root}" 2>/dev/null | tail -1 | awk '{print $3}')

            if [ -n "${disk_used_output}" ]; then
                disk_used_gb=$(echo "${disk_used_output}" | sed 's/G$//')
                disk_usage_percent=$(awk -v used="$disk_used_gb" -v alloc="$allocated_disk_gb" \
                    'BEGIN { printf "%.2f", (used / alloc) * 100 }')

                echo "Actual Max Disk Used: ${disk_used_gb} GB"
                echo "Disk Usage: ${disk_usage_percent}%"
            else
                echo "Could not measure disk usage"
            fi
        else
            echo "df command not available"
        fi

        echo ""
        echo "==========================================="
    >>>

    output {
        File htrms_file = glob("*.htrms")[0]
        Float htrms_size_gb = read_float("htrms_size_gb.txt")
    }

    runtime {
        docker: "broadcptacdev/panoply_spectronaut:v20.3"
        cpuPlatform: "AMD Rome"
        predefinedMachineType: "n2d-standard-16"
        cpu: 16
        memory: "64GB"
        bootDiskSizeGb: 50
        disks: "local-disk ~{disk_size_gb} HDD"
        preemptible: n_preemptible
    }
}

task directDIA_single_vm {
    input {
        File fasta_1
        Array[File] input_files
        String experiment_name
        Float total_size_gb
        Int disk_size_multiplier
        Int cpu
        Int ram_gb
        Int allocated_disk_gb
        Int n_preemptible
        String predefined_machine_type
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
    }

    command <<<
        set -euo pipefail

        cromwell_root=$(pwd)

        # Resource Monitoring Initialization
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

        input_dir="${cromwell_root}/work_input"
        mkdir -p "${input_dir}"

        output_dir="${cromwell_root}/out_spectronaut"
        mkdir -p "${output_dir}"

        tmp_dir="${cromwell_root}/sn_temp"
        mkdir -p "${tmp_dir}"

        output_zip="${cromwell_root}/spectronaut_output.zip"

        # Copy all input files to input directory
        echo "Copying input files to input directory..."
        while IFS= read -r input_file; do
            if [ -n "${input_file}" ]; then
                if [ -d "${input_file}" ]; then
                    cp -r "${input_file}" "${input_dir}/"
                elif [ -f "${input_file}" ]; then
                    cp "${input_file}" "${input_dir}/"
                fi
            fi
        done < ~{write_lines(input_files)}

        # Verify files were copied
        file_count=$(find "${input_dir}" -type f | wc -l)
        echo "Copied ${file_count} files to input directory"

        # Import enzyme database if provided
        if [ ~{defined(enzyme_database)} = true ]; then
            echo "Importing enzyme database..."
            dotnet SpectronautCMD.dll --importEnzymeDB "~{enzyme_database}"
        fi

        # Run directDIA search
        echo "Starting directDIA search..."
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

        echo "directDIA search complete."

        # ============================================================================
        # Resource Usage Report
        # ============================================================================
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

        if [ "$cpu_start_ns" -gt 0 ] && [ "$cpu_end_ns" -gt "$cpu_start_ns" ] && [ "$wall_elapsed_ns" -gt 0 ]; then
            cpu_used_ns=$((cpu_end_ns - cpu_start_ns))
            cpu_utilization_total=$(awk -v cpu="$cpu_used_ns" -v wall="$wall_elapsed_ns" \
                'BEGIN { printf "%.2f", (cpu / wall) * 100 }')
            allocated_cpus=~{cpu}
            cpu_utilization_per_core=$(awk -v total="$cpu_utilization_total" -v cpus="$allocated_cpus" \
                'BEGIN { printf "%.2f", total / cpus }')

            echo "Allocated CPUs: ${allocated_cpus}"
            echo "Total CPU Utilization: ${cpu_utilization_total}% (across all cores)"
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

            if [ "$limit_bytes" -gt 0 ] && [ "$limit_bytes" != "max" ]; then
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

        allocated_disk_gb=~{allocated_disk_gb}
        echo "Disk Size Assigned: ${allocated_disk_gb} GB"

        if command -v df >/dev/null 2>&1; then
            disk_used_output=$(df -BG "${cromwell_root}" 2>/dev/null | tail -1 | awk '{print $3}')

            if [ -n "${disk_used_output}" ]; then
                disk_used_gb=$(echo "${disk_used_output}" | sed 's/G$//')
                disk_usage_percent=$(awk -v used="$disk_used_gb" -v alloc="$allocated_disk_gb" \
                    'BEGIN { printf "%.2f", (used / alloc) * 100 }')

                echo "Actual Max Disk Used: ${disk_used_gb} GB"
                echo "Disk Usage: ${disk_usage_percent}%"
            else
                echo "Could not measure disk usage"
            fi
        else
            echo "df command not available"
        fi

        echo ""
        echo "==========================================="
    >>>

    output {
        File spectronaut_output = "spectronaut_output.zip"
    }

    runtime {
        docker: "broadcptacdev/panoply_spectronaut:v20.3"
        predefinedMachineType: predefined_machine_type
        cpu: cpu
        memory: "~{ram_gb}GB"
        bootDiskSizeGb: 50
        disks: "local-disk ~{ceil(total_size_gb * disk_size_multiplier)} HDD"
        preemptible: n_preemptible
        cpuPlatform: "AMD Rome"
    }
}

task directDIA_search_binned {
    input {
        File fasta_1
        Array[File] input_files
        Float bin_size_gb
        Int disk_size_multiplier
        Int cpu
        Int ram_gb
        Int bin_index
        Int allocated_disk_gb
        Int n_preemptible
        String predefined_machine_type
        File? analysis_schema
        File? fasta_2
        File? fasta_3
        File? enzyme_database
    }

    command <<<
        set -euo pipefail

        cromwell_root=$(pwd)

        # Resource Monitoring Initialization
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

        input_dir="${cromwell_root}/work_input"
        mkdir -p "${input_dir}"

        output_dir="${cromwell_root}/out_archive"
        mkdir -p "${output_dir}"

        tmp_dir="${cromwell_root}/sn_temp"
        mkdir -p "${tmp_dir}"

        # Copy all input files to input directory
        echo "Copying input files..."
        while IFS= read -r input_file; do
            if [ -n "${input_file}" ]; then
                if [ -d "${input_file}" ]; then
                    cp -r "${input_file}" "${input_dir}/"
                else
                    cp "${input_file}" "${input_dir}/"
                fi
            fi
        done < ~{write_lines(input_files)}

        # Import enzyme database if provided
        if [ ~{defined(enzyme_database)} = true ]; then
            echo "Importing enzyme database..."
            dotnet SpectronautCMD.dll --importEnzymeDB "~{enzyme_database}"
        fi

        # Process each file individually for search archive generation
        echo "Processing files individually for search archive generation..."
        file_count=0
        for input_file in "${input_dir}"/*; do
            # Skip if not a regular file
            if [ ! -f "${input_file}" ]; then
                continue
            fi

            # Extract basename without any extension
            full_basename=$(basename "${input_file}")
            base_name="${full_basename%.*}"

            # Create individual temp directories
            file_input_dir="${cromwell_root}/input_${base_name}"
            file_output_dir="${cromwell_root}/output_${base_name}"
            file_tmp_dir="${cromwell_root}/temp_${base_name}"

            mkdir -p "${file_input_dir}" "${file_output_dir}" "${file_tmp_dir}"

            # Copy single file
            cp "${input_file}" "${file_input_dir}/"

            echo "Processing ${base_name}..."
            spectronaut lg -se Pulsar \
                -d "${file_input_dir}" \
                -fasta "~{fasta_1}" \
                ~{if defined(fasta_2) then "-fasta " + fasta_2 else ""} \
                ~{if defined(fasta_3) then "-fasta " + fasta_3 else ""} \
                ~{if defined(analysis_schema) then "-s " + analysis_schema else ""} \
                -a "${file_output_dir}/search_archive_bin_~{bin_index}_${base_name}.psar" \
                -o "${file_output_dir}" \
                -setTemp "${file_tmp_dir}" 2>&1 | tee "archive_${base_name}.log"

            # Find and rename the .psar file
            psar_file=$(find "${file_output_dir}" -type f -name "*.psar" | head -n 1)

            if [ -z "${psar_file}" ] || [ ! -f "${psar_file}" ]; then
                echo "ERROR: No .psar file produced for ${base_name}" >&2
                exit 1
            fi

            # Move to cromwell root with unique name
            output_name="search_archive_bin_~{bin_index}_${base_name}.psar"
            mv "${psar_file}" "${cromwell_root}/${output_name}"
            echo "Generated ${output_name}"

            file_count=$((file_count + 1))
        done

        if [ "${file_count}" -eq 0 ]; then
            echo "ERROR: No files were processed successfully" >&2
            exit 1
        fi

        echo "Archive generation complete. Generated ${file_count} search archives for bin ~{
            bin_index}"

        # ============================================================================
        # Resource Usage Report
        # ============================================================================
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

        if [ "$cpu_start_ns" -gt 0 ] && [ "$cpu_end_ns" -gt "$cpu_start_ns" ] && [ "$wall_elapsed_ns" -gt 0 ]; then
            cpu_used_ns=$((cpu_end_ns - cpu_start_ns))
            cpu_utilization_total=$(awk -v cpu="$cpu_used_ns" -v wall="$wall_elapsed_ns" \
                'BEGIN { printf "%.2f", (cpu / wall) * 100 }')
            allocated_cpus=~{cpu}
            cpu_utilization_per_core=$(awk -v total="$cpu_utilization_total" -v cpus="$allocated_cpus" \
                'BEGIN { printf "%.2f", total / cpus }')

            echo "Allocated CPUs: ${allocated_cpus}"
            echo "Total CPU Utilization: ${cpu_utilization_total}% (across all cores)"
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

            if [ "$limit_bytes" -gt 0 ] && [ "$limit_bytes" != "max" ]; then
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

        allocated_disk_gb=~{allocated_disk_gb}
        echo "Disk Size Assigned: ${allocated_disk_gb} GB"

        if command -v df >/dev/null 2>&1; then
            disk_used_output=$(df -BG "${cromwell_root}" 2>/dev/null | tail -1 | awk '{print $3}')

            if [ -n "${disk_used_output}" ]; then
                disk_used_gb=$(echo "${disk_used_output}" | sed 's/G$//')
                disk_usage_percent=$(awk -v used="$disk_used_gb" -v alloc="$allocated_disk_gb" \
                    'BEGIN { printf "%.2f", (used / alloc) * 100 }')

                echo "Actual Max Disk Used: ${disk_used_gb} GB"
                echo "Disk Usage: ${disk_usage_percent}%"
            else
                echo "Could not measure disk usage"
            fi
        else
            echo "df command not available"
        fi

        echo ""
        echo "==========================================="
    >>>

    output {
        Array[File] search_archives = glob("*.psar")
    }

    runtime {
        docker: "broadcptacdev/panoply_spectronaut:v20.3"
        predefinedMachineType: predefined_machine_type
        cpu: cpu
        memory: "~{ram_gb}GB"
        bootDiskSizeGb: 50
        disks: "local-disk ~{ceil(bin_size_gb * disk_size_multiplier)} HDD"
        preemptible: n_preemptible
        cpuPlatform: "AMD Rome"
    }
}

task combine_archives {
    input {
        Array[File] input_archives
        Float total_input_size_gb
        Int disk_size_multiplier
        Int cpu
        Int ram_gb
        Int allocated_disk_gb
        Int n_preemptible
        String predefined_machine_type
        File? enzyme_database
    }

    command <<<
        set -euo pipefail

        cromwell_root=$(pwd)

        # Resource Monitoring Initialization
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
            dotnet SpectronautCMD.dll --importEnzymeDB "~{enzyme_database}"
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

        # ============================================================================
        # Resource Usage Report
        # ============================================================================
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

        if [ "$cpu_start_ns" -gt 0 ] && [ "$cpu_end_ns" -gt "$cpu_start_ns" ] && [ "$wall_elapsed_ns" -gt 0 ]; then
            cpu_used_ns=$((cpu_end_ns - cpu_start_ns))
            cpu_utilization_total=$(awk -v cpu="$cpu_used_ns" -v wall="$wall_elapsed_ns" \
                'BEGIN { printf "%.2f", (cpu / wall) * 100 }')
            allocated_cpus=~{cpu}
            cpu_utilization_per_core=$(awk -v total="$cpu_utilization_total" -v cpus="$allocated_cpus" \
                'BEGIN { printf "%.2f", total / cpus }')

            echo "Allocated CPUs: ${allocated_cpus}"
            echo "Total CPU Utilization: ${cpu_utilization_total}% (across all cores)"
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

            if [ "$limit_bytes" -gt 0 ] && [ "$limit_bytes" != "max" ]; then
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

        allocated_disk_gb=~{allocated_disk_gb}
        echo "Disk Size Assigned: ${allocated_disk_gb} GB"

        if command -v df >/dev/null 2>&1; then
            disk_used_output=$(df -BG "${cromwell_root}" 2>/dev/null | tail -1 | awk '{print $3}')

            if [ -n "${disk_used_output}" ]; then
                disk_used_gb=$(echo "${disk_used_output}" | sed 's/G$//')
                disk_usage_percent=$(awk -v used="$disk_used_gb" -v alloc="$allocated_disk_gb" \
                    'BEGIN { printf "%.2f", (used / alloc) * 100 }')

                echo "Actual Max Disk Used: ${disk_used_gb} GB"
                echo "Disk Usage: ${disk_usage_percent}%"
            else
                echo "Could not measure disk usage"
            fi
        else
            echo "df command not available"
        fi

        echo ""
        echo "==========================================="
    >>>

    output {
        File merged_archive = "merged_library.kit"
    }

    runtime {
        docker: "broadcptacdev/panoply_spectronaut:v20.3"
        predefinedMachineType: predefined_machine_type
        cpu: cpu
        memory: "~{ram_gb}GB"
        bootDiskSizeGb: 50
        disks: "local-disk ~{ceil(total_input_size_gb * disk_size_multiplier)} HDD"
        preemptible: n_preemptible
        cpuPlatform: "AMD Rome"
    }
}

task dia_analysis_binned {
    input {
        File search_archive
        File fasta_1
        Array[File] input_files
        String experiment_name
        Float bin_size_gb
        Int disk_size_multiplier
        Int cpu
        Int ram_gb
        Int bin_index
        Int allocated_disk_gb
        Int n_preemptible
        String predefined_machine_type
        File? enzyme_database
        File? analysis_schema
        File? fasta_2
        File? fasta_3
        File? json_settings
    }

    command <<<
        set -euo pipefail

        cromwell_root=$(pwd)

        # Resource Monitoring Initialization
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

        input_dir="${cromwell_root}/work_input"
        mkdir -p "${input_dir}"

        output_dir="${cromwell_root}/out_dia"
        mkdir -p "${output_dir}"

        tmp_dir="${cromwell_root}/work_dia_temp"
        mkdir -p "${tmp_dir}"

        # Copy all input files to input directory
        echo "Copying input files..."
        while IFS= read -r input_file; do
            if [ -n "${input_file}" ]; then
                if [ -d "${input_file}" ]; then
                    cp -r "${input_file}" "${input_dir}/"
                elif [ -f "${input_file}" ]; then
                    cp "${input_file}" "${input_dir}/"
                fi
            fi
        done < ~{write_lines(input_files)}

        # Import enzyme database if provided
        if [ ~{defined(enzyme_database)} = true ]; then
            echo "Importing enzyme database..."
            dotnet SpectronautCMD.dll --importEnzymeDB "~{enzyme_database}"
        fi

        # Process each file individually for DIA analysis
        echo "Processing files individually for DIA analysis..."
        file_count=0
        for input_file in "${input_dir}"/*; do
            # Skip if not a regular file
            if [ ! -f "${input_file}" ]; then
                continue
            fi

            # Extract basename without any extension
            full_basename=$(basename "${input_file}")
            base_name="${full_basename%.*}"

            # Create individual temp directories
            file_input_dir="${cromwell_root}/input_${base_name}"
            file_output_dir="${cromwell_root}/output_${base_name}"
            file_tmp_dir="${cromwell_root}/temp_${base_name}"

            mkdir -p "${file_input_dir}" "${file_output_dir}" "${file_tmp_dir}"

            # Copy single file
            cp "${input_file}" "${file_input_dir}/"

            echo "Processing ${base_name} for DIA analysis..."
            spectronaut diaanalysis \
                ~{if defined(analysis_schema) then "-s " + analysis_schema else ""} \
                -fasta "~{fasta_1}" \
                ~{if defined(fasta_2) then "-fasta " + fasta_2 else ""} \
                ~{if defined(fasta_3) then "-fasta " + fasta_3 else ""} \
                ~{if defined(json_settings) then "-j " + json_settings else ""} \
                -n "~{experiment_name}_bin_~{bin_index}_${base_name}" \
                -o "${file_output_dir}" \
                -d "${file_input_dir}" \
                -a "~{search_archive}" \
                -setTemp "${file_tmp_dir}" 2>&1 | tee "dia_analysis_${base_name}.log"

            # Find and rename the .sne file
            sne_file=$(find "${file_output_dir}" -type f -name "*.sne" | head -n 1)

            if [ -z "${sne_file}" ] || [ ! -f "${sne_file}" ]; then
                echo "ERROR: No .sne file produced for ${base_name}" >&2
                exit 1
            fi

            # Move to cromwell root with unique name
            output_name="~{experiment_name}_bin_~{bin_index}_${base_name}.sne"
            mv "${sne_file}" "${cromwell_root}/${output_name}"
            echo "Generated ${output_name}"

            file_count=$((file_count + 1))
        done

        if [ "${file_count}" -eq 0 ]; then
            echo "ERROR: No files were processed successfully" >&2
            exit 1
        fi

        echo "DIA analysis complete. Generated ${file_count} SNE files for bin ~{
            bin_index}"

        # ============================================================================
        # Resource Usage Report
        # ============================================================================
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

        if [ "$cpu_start_ns" -gt 0 ] && [ "$cpu_end_ns" -gt "$cpu_start_ns" ] && [ "$wall_elapsed_ns" -gt 0 ]; then
            cpu_used_ns=$((cpu_end_ns - cpu_start_ns))
            cpu_utilization_total=$(awk -v cpu="$cpu_used_ns" -v wall="$wall_elapsed_ns" \
                'BEGIN { printf "%.2f", (cpu / wall) * 100 }')
            allocated_cpus=~{cpu}
            cpu_utilization_per_core=$(awk -v total="$cpu_utilization_total" -v cpus="$allocated_cpus" \
                'BEGIN { printf "%.2f", total / cpus }')

            echo "Allocated CPUs: ${allocated_cpus}"
            echo "Total CPU Utilization: ${cpu_utilization_total}% (across all cores)"
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

            if [ "$limit_bytes" -gt 0 ] && [ "$limit_bytes" != "max" ]; then
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

        allocated_disk_gb=~{allocated_disk_gb}
        echo "Disk Size Assigned: ${allocated_disk_gb} GB"

        if command -v df >/dev/null 2>&1; then
            disk_used_output=$(df -BG "${cromwell_root}" 2>/dev/null | tail -1 | awk '{print $3}')

            if [ -n "${disk_used_output}" ]; then
                disk_used_gb=$(echo "${disk_used_output}" | sed 's/G$//')
                disk_usage_percent=$(awk -v used="$disk_used_gb" -v alloc="$allocated_disk_gb" \
                    'BEGIN { printf "%.2f", (used / alloc) * 100 }')

                echo "Actual Max Disk Used: ${disk_used_gb} GB"
                echo "Disk Usage: ${disk_usage_percent}%"
            else
                echo "Could not measure disk usage"
            fi
        else
            echo "df command not available"
        fi

        echo ""
        echo "==========================================="
    >>>

    output {
        Array[File] sne_files = glob("*.sne")
    }

    runtime {
        docker: "broadcptacdev/panoply_spectronaut:v20.3"
        predefinedMachineType: predefined_machine_type
        cpu: cpu
        memory: "~{ram_gb}GB"
        bootDiskSizeGb: 50
        disks: "local-disk ~{ceil(bin_size_gb * disk_size_multiplier)} HDD"
        preemptible: n_preemptible
        cpuPlatform: "AMD Rome"
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
        Int allocated_disk_gb
        Int n_preemptible
        String predefined_machine_type
        File? condition_setup
        File? report_schema_1
        File? report_schema_2
        File? report_schema_3
        File? report_schema_4
        File? analysis_schema
        File? enzyme_database
    }

    command <<<
        set -euo pipefail

        cromwell_root=$(pwd)

        # Resource Monitoring Initialization
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
            dotnet SpectronautCMD.dll --importEnzymeDB "~{enzyme_database}"
        fi

        spectronaut manageSNE --merge \
            -n "~{experiment_name}" \
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

        # ============================================================================
        # Resource Usage Report
        # ============================================================================
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

        if [ "$cpu_start_ns" -gt 0 ] && [ "$cpu_end_ns" -gt "$cpu_start_ns" ] && [ "$wall_elapsed_ns" -gt 0 ]; then
            cpu_used_ns=$((cpu_end_ns - cpu_start_ns))
            cpu_utilization_total=$(awk -v cpu="$cpu_used_ns" -v wall="$wall_elapsed_ns" \
                'BEGIN { printf "%.2f", (cpu / wall) * 100 }')
            allocated_cpus=~{cpu}
            cpu_utilization_per_core=$(awk -v total="$cpu_utilization_total" -v cpus="$allocated_cpus" \
                'BEGIN { printf "%.2f", total / cpus }')

            echo "Allocated CPUs: ${allocated_cpus}"
            echo "Total CPU Utilization: ${cpu_utilization_total}% (across all cores)"
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

            if [ "$limit_bytes" -gt 0 ] && [ "$limit_bytes" != "max" ]; then
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

        allocated_disk_gb=~{allocated_disk_gb}
        echo "Disk Size Assigned: ${allocated_disk_gb} GB"

        if command -v df >/dev/null 2>&1; then
            disk_used_output=$(df -BG "${cromwell_root}" 2>/dev/null | tail -1 | awk '{print $3}')

            if [ -n "${disk_used_output}" ]; then
                disk_used_gb=$(echo "${disk_used_output}" | sed 's/G$//')
                disk_usage_percent=$(awk -v used="$disk_used_gb" -v alloc="$allocated_disk_gb" \
                    'BEGIN { printf "%.2f", (used / alloc) * 100 }')

                echo "Actual Max Disk Used: ${disk_used_gb} GB"
                echo "Disk Usage: ${disk_usage_percent}%"
            else
                echo "Could not measure disk usage"
            fi
        else
            echo "df command not available"
        fi

        echo ""
        echo "==========================================="
    >>>

    output {
        File spectronaut_output = "spectronaut_output.zip"
    }

    runtime {
        docker: "broadcptacdev/panoply_spectronaut:v20.3"
        predefinedMachineType: predefined_machine_type
        cpu: cpu
        memory: "~{ram_gb}GB"
        bootDiskSizeGb: 50
        disks: "local-disk ~{ceil(total_input_size_gb * disk_size_multiplier)} HDD"
        preemptible: n_preemptible
        cpuPlatform: "AMD Rome"
    }
}

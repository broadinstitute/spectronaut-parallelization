version development

# Major fix of a fatal error in "parallel_spectronaut_v5_bin.wdl" - duplicated names for search archives across shards

# Dynamic VM Binning with Size-based Disk Allocation
# Files are distributed across n VMs using round-robin binning
# Disk sizes are calculated dynamically based on actual input file sizes
workflow parallel_spectronaut {
    input {
        # REQUIRED CORE INPUTS
        File fasta_1
        String experiment_name
        String file_directory

        # CONFIGURATION (with defaults)
        Int num_vms = 10  # Number of VMs to use for parallel processing (default: 10, auto-adjusted based on file count)
        Boolean do_conversion = true  # If true, convert raw files to HTRMS; if false, use raw files directly
        String experiment_type = "proteome"  # proteome / ptm
        Int disk_size_multiplier = 5  # Multiplier for disk size calculation

        # ADDITIONAL FASTA DATABASES
        File? fasta_2
        File? fasta_3

        # DATABASE & SCHEMA FILES
        File? enzyme_database
        File? convert_schema
        File? directDIA_settings  # For search archive generation and DIA analysis

        # REPORT & OUTPUT CONFIGURATION
        File? condition_setup
        File? json_settings
        File? report_schema_1
        File? report_schema_2
        File? report_schema_3
        File? report_schema_4

        # PREEMPTIBLE SETTINGS
        Int n_preemptible_htrms_conversion = 0
        Int n_preemptible_directDIA_search = 0
        Int n_preemptible_combine_archives = 0
        Int n_preemptible_dia_analysis = 0
        Int n_preemptible_sne_merge = 0
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

    Map[String, Int] sne_merge_cpu_presets = {
        "proteome": 16,
        "ptm": 16,
    }
    Map[String, Int] sne_merge_ram_gb_presets = {
        "proteome": 40,
        "ptm": 80,
    }

    # Look up values based on experiment_type
    Int directDIA_search_cpu = directDIA_search_cpu_presets[experiment_type]
    Int directDIA_search_ram_gb = directDIA_search_ram_gb_presets[experiment_type]

    Int combine_archives_cpu = combine_archives_cpu_presets[experiment_type]
    Int combine_archives_ram_gb = combine_archives_ram_gb_presets[experiment_type]

    Int dia_analysis_cpu = dia_analysis_cpu_presets[experiment_type]
    Int dia_analysis_ram_gb = dia_analysis_ram_gb_presets[experiment_type]

    Int sne_merge_cpu = sne_merge_cpu_presets[experiment_type]
    Int sne_merge_ram_gb = sne_merge_ram_gb_presets[experiment_type]

    # PHASE 1: List all files in the provided directory
    call list_files { input:
        gcs_path = file_directory,
    }

    Array[String] file_paths = read_lines(list_files.file_list)
    Int num_files = length(file_paths)

    # PHASE 2: Determine actual num_vms based on file count and user input
    call determine_num_vms { input:
        requested_num_vms = num_vms,
        num_files = num_files,
    }

    Int actual_num_vms = determine_num_vms.actual_num_vms

    # PHASE 3: Download and convert all files individually (one file per VM)
    scatter (file_path in file_paths) {
        call download_and_convert_single { input:
            file_path = file_path,
            do_conversion = do_conversion,
            convert_schema = convert_schema,
            n_preemptible = n_preemptible_htrms_conversion,
        }
    }

    # Pool all converted/downloaded files
    Array[File] all_converted_files = download_and_convert_single.output_file
    Array[Float] file_sizes_gb = download_and_convert_single.file_size_gb

    # Calculate total size for disk allocation
    call sum_floats as sum_all_sizes { input:
        values = file_sizes_gb,
    }

    Float total_size_gb = sum_all_sizes.total
    Float per_bin_size_gb = total_size_gb / actual_num_vms

    # PHASE 4: Branch based on actual_num_vms
    if (actual_num_vms == 1) {
        # Single VM path: Direct search producing final output (no two-step process)
        call directDIA_search_and_analyze_single { input:
            experiment_name = experiment_name,
            input_files = all_converted_files,
            total_size_gb = total_size_gb,
            fasta_1 = fasta_1,
            fasta_2 = fasta_2,
            fasta_3 = fasta_3,
            enzyme_database = enzyme_database,
            directDIA_settings = directDIA_settings,
            condition_setup = condition_setup,
            report_schema_1 = report_schema_1,
            report_schema_2 = report_schema_2,
            report_schema_3 = report_schema_3,
            report_schema_4 = report_schema_4,
            json_settings = json_settings,
            disk_size_multiplier = disk_size_multiplier,
            cpu = directDIA_search_cpu,
            ram_gb = directDIA_search_ram_gb,
        }
    }

    if (actual_num_vms > 1) {
        # PHASE 5: Create bins for parallel processing
        call write_array_to_file { input:
            file_paths = all_converted_files,
        }

        call create_bins_from_file { input:
            file_paths_file = write_array_to_file.output_file,
            num_bins = actual_num_vms,
        }

        Array[Array[String]] file_bins_paths = read_json(create_bins_from_file.bins_json)

        # PHASE 6:directDIA search for each bin to generate search archives
        scatter (bin_idx in range(length(file_bins_paths))) {
            call convert_paths_to_files as convert_for_search { input:
                file_paths = file_bins_paths[bin_idx],
            }

            call directDIA_search_binned { input:
                input_files = convert_for_search.files,
                analysis_settings = directDIA_settings,
                fasta_1 = fasta_1,
                fasta_2 = fasta_2,
                fasta_3 = fasta_3,
                enzyme_database = enzyme_database,
                cpu = directDIA_search_cpu,
                ram_gb = directDIA_search_ram_gb,
                n_preemptible = n_preemptible_directDIA_search,
                bin_size_gb = per_bin_size_gb,
                disk_size_multiplier = disk_size_multiplier,
                bin_index = bin_idx,
            }
        }

        # PHASE 7: Combine scattered search archives into one library
        Array[File] all_archives = flatten(directDIA_search_binned.search_archives)

        call combine_archives { input:
            input_archives = all_archives,
            total_input_size_gb = total_size_gb,
            disk_size_multiplier = disk_size_multiplier,
            cpu = combine_archives_cpu,
            ram_gb = combine_archives_ram_gb,
            n_preemptible = n_preemptible_combine_archives,
        }

        # PHASE 8: DIA analysis for each bin against the merged library
        scatter (bin_idx in range(length(file_bins_paths))) {
            call convert_paths_to_files as convert_for_analysis { input:
                file_paths = file_bins_paths[bin_idx],
            }

            call dia_analysis_binned { input:
                experiment_name = experiment_name,
                input_files = convert_for_analysis.files,
                search_archive = combine_archives.merged_archive,
                analysis_settings = directDIA_settings,
                fasta_1 = fasta_1,
                fasta_2 = fasta_2,
                fasta_3 = fasta_3,
                json_settings = json_settings,
                cpu = dia_analysis_cpu,
                ram_gb = dia_analysis_ram_gb,
                bin_index = bin_idx,
                bin_size_gb = per_bin_size_gb,
                disk_size_multiplier = disk_size_multiplier,
                n_preemptible = n_preemptible_dia_analysis,
            }
        }

        # PHASE 9: Merge scattered SNE files and generate reports
        Array[File] all_sne = flatten(dia_analysis_binned.sne_files)

        call sne_merge { input:
            experiment_name = experiment_name,
            sne_files = all_sne,
            analysis_settings = directDIA_settings,
            condition_setup = condition_setup,
            report_schema_1 = report_schema_1,
            report_schema_2 = report_schema_2,
            report_schema_3 = report_schema_3,
            report_schema_4 = report_schema_4,
            cpu = sne_merge_cpu,
            ram_gb = sne_merge_ram_gb,
            total_input_size_gb = total_size_gb,
            disk_size_multiplier = disk_size_multiplier,
            n_preemptible = n_preemptible_sne_merge,
        }
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
        disks: "local-disk 200 HDD"
        preemptible: 2
    }
}

task determine_num_vms {
    input {
        Int requested_num_vms
        Int num_files
    }

    command <<<
        python3 <<CODE
requested = ~{requested_num_vms}
num_files = ~{num_files}

# Condition (i): if num_files < 10, set num_vms = num_files
if num_files < 10:
    actual = num_files
    message = f"INFO: Setting num_vms={actual} (number of input files) because total files ({num_files}) < 10"
# Condition (ii): if user set num_vms=1 but has >25 files, override to 10
elif requested == 1 and num_files > 25:
    actual = 10
    message = f"WARNING: Overriding num_vms from 1 to 10 for efficient runtime with {num_files} files (>25 threshold)"
else:
    actual = requested
    message = f"INFO: Using requested num_vms={actual} for {num_files} files"

print(message, flush=True)

with open("actual_num_vms.txt", "w") as f:
    f.write(str(actual))

with open("message.txt", "w") as f:
    f.write(message)
CODE
    >>>

    output {
        Int actual_num_vms = read_int("actual_num_vms.txt")
        String message = read_string("message.txt")
    }

    runtime {
        docker: "python:3.9-slim"
        cpu: 2
        memory: "4GB"
        bootDiskSizeGb: 10
        disks: "local-disk 50 HDD"
        preemptible: 2
    }
}

task download_and_convert_single {
    input {
        String file_path
        Boolean do_conversion
        File? convert_schema
        Int n_preemptible = 0
    }

    command <<<
        set -euo pipefail

        cromwell_root=$(pwd)

        # Download file from GCS
        echo "Downloading file: ~{file_path}"
        gcloud storage cp "~{file_path}" "${cromwell_root}/"

        filename=$(basename "~{file_path}")
        downloaded_file="${cromwell_root}/${filename}"

        if [ ! -f "${downloaded_file}" ]; then
            echo "ERROR: Failed to download file" >&2
            exit 1
        fi

        # Conditionally convert to HTRMS
        if [ "~{do_conversion}" = "true" ]; then
            echo "Converting to HTRMS..."

            input_dir="${cromwell_root}/work_input"
            mkdir -p "${input_dir}"
            mv "${downloaded_file}" "${input_dir}/"

            output_dir="${cromwell_root}/out_conversion"
            mkdir -p "${output_dir}"

            tmp_dir="${cromwell_root}/sn_temp"
            mkdir -p "${tmp_dir}"

            spectronaut -convert \
                -i "${input_dir}" \
                -o "${output_dir}" \
                ~{if defined(convert_schema) then "-s " + convert_schema else ""} \
                -setTemp "${tmp_dir}" 2>&1 | tee htrms_conversion.log

            # Move HTRMS file to output
            htrms_file=$(find "${output_dir}" -type f -name "*.htrms" | head -n 1)
            if [ -z "${htrms_file}" ]; then
                echo "ERROR: No .htrms file produced" >&2
                exit 1
            fi

            mv "${htrms_file}" "${cromwell_root}/"
            output_file=$(basename "${htrms_file}")
        else
            echo "Skipping conversion, using raw file"
            output_file="${filename}"
        fi

        # Calculate file size in GB and write output file path
        echo "Calculating file size..."
        file_size_bytes=$(du -b "${cromwell_root}/${output_file}" | awk '{print $1}')
        file_size_gb=$(awk "BEGIN {printf \"%.2f\", ${file_size_bytes} / (1024^3)}")
        echo "${file_size_gb}" > "${cromwell_root}/file_size_gb.txt"
        echo "${output_file}" > "${cromwell_root}/output_filename.txt"
        echo "File size: ${file_size_gb} GB"
        echo "Output file: ${output_file}"
    >>>

    output {
        File output_file = read_string("output_filename.txt")
        Float file_size_gb = read_float("file_size_gb.txt")
    }

    runtime {
        docker: "cameronlian/panoply-spectronaut:v20.3"
        cpu: 16
        memory: "32GB"
        bootDiskSizeGb: 64
        disks: "local-disk 500 HDD"
        preemptible: n_preemptible
    }
}

task create_bins_from_file {
    input {
        File file_paths_file
        Int num_bins
    }

    command <<<
                python3 <<CODE
import json

# Read file paths from file
with open("~{file_paths_file}") as f:
    files = [line.strip() for line in f if line.strip()]

num_bins = ~{num_bins}

# Validate num_bins
if num_bins < 1:
    raise ValueError(f"num_bins must be at least 1, got {num_bins}")

# Create bins using round-robin distribution
bins = [[] for _ in range(num_bins)]
for i, file_path in enumerate(files):
    bin_index = i % num_bins
    bins[bin_index].append(file_path)

# Write bins to JSON
with open("bins.json", "w") as f:
    json.dump(bins, f, indent=2)

# Print summary
print(f"Total files: {len(files)}")
print(f"Number of bins: {num_bins}")
for i, bin_files in enumerate(bins):
    print(f"Bin {i}: {len(bin_files)} files")
CODE
    >>>

    output {
        File bins_json = "bins.json"
    }

    runtime {
        docker: "python:3.9-slim"
        cpu: 2
        memory: "8GB"
        bootDiskSizeGb: 20
        disks: "local-disk 200 HDD"
        preemptible: 2
    }
}

task convert_paths_to_files {
    input {
        Array[String] file_paths
    }

    command <<<
        # Write paths to file, which will be read back as File objects for localization
        while IFS= read -r path; do
            echo "${path}"
        done < ~{write_lines(file_paths)} > file_list.txt
    >>>

    output {
        Array[File] files = read_lines("file_list.txt")
    }

    runtime {
        docker: "google/cloud-sdk:slim"
        cpu: 2
        memory: "4GB"
        bootDiskSizeGb: 10
        disks: "local-disk 50 HDD"
        preemptible: 2
    }
}

task directDIA_search_and_analyze_single {
    input {
        # Primary data inputs
        String experiment_name
        Array[File] input_files
        File fasta_1

        # Additional FASTA databases
        File? fasta_2
        File? fasta_3

        # Database & schema files
        File? enzyme_database
        File? directDIA_settings

        # Report & output configuration
        File? condition_setup
        File? json_settings
        File? report_schema_1
        File? report_schema_2
        File? report_schema_3
        File? report_schema_4

        # Resource parameters
        Int cpu
        Int ram_gb
        Float total_size_gb
        Int disk_size_multiplier
    }

    command <<<
        set -euo pipefail

        cromwell_root=$(pwd)

        input_dir="${cromwell_root}/work_input"
        mkdir -p "${input_dir}"

        output_dir="${cromwell_root}/out"
        mkdir -p "${output_dir}"

        tmp_dir="${cromwell_root}/sn_temp"
        mkdir -p "${tmp_dir}"

        # Copy all input files
        echo "Copying input files..."
        while IFS= read -r input_file; do
            if [ -n "${input_file}" ] && [ -f "${input_file}" ]; then
                cp "${input_file}" "${input_dir}/"
            fi
        done < ~{write_lines(input_files)}

        # Import enzyme database if provided
        if [ ~{defined(enzyme_database)} = true ]; then
            echo "Importing enzyme database..."
            dotnet /usr/lib/spectronaut/SpectronautCMD.dll --importEnzymeDB "~{enzyme_database}"
        fi

        # Run SpectronautdirectDIA (single-step search producing final output)
        echo "Starting directDIA search (single VM, no parallelization)..."
        spectronaut direct \
            -d "${input_dir}" \
            -fasta "~{fasta_1}" \
            ~{if defined(fasta_2) then "-fasta " + fasta_2 else ""} \
            ~{if defined(fasta_3) then "-fasta " + fasta_3 else ""} \
            ~{if defined(directDIA_settings) then "-s " + directDIA_settings else ""} \
            ~{if defined(condition_setup) then "-con " + condition_setup else ""} \
            ~{if defined(report_schema_1) then "-rs " + report_schema_1 else ""} \
            ~{if defined(report_schema_2) then "-rs " + report_schema_2 else ""} \
            ~{if defined(report_schema_3) then "-rs " + report_schema_3 else ""} \
            ~{if defined(report_schema_4) then "-rs " + report_schema_4 else ""} \
            ~{if defined(json_settings) then "-j " + json_settings else ""} \
            -n "~{experiment_name}" \
            -o "${output_dir}" \
            -setTemp "${tmp_dir}" 2>&1 | tee spectronaut_single.log

        # Create output archive
        echo "Creating output archive..."
        zip -r spectronaut_output.zip "${output_dir}" -x \\*.zip

        if [ ! -f "spectronaut_output.zip" ]; then
            echo "ERROR: Failed to create output zip file" >&2
            exit 1
        fi

        echo "directDIA search complete!"

        # Memory usage reporting
        echo "=== Memory Usage Report ==="
        if [ -f /sys/fs/cgroup/memory.peak ]; then
            max_mem_bytes=$(cat /sys/fs/cgroup/memory.peak)
            limit_bytes=$(cat /sys/fs/cgroup/memory.max)
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
        disks: "local-disk ~{ceil(total_size_gb * disk_size_multiplier)} HDD"
        preemptible: 0
    }
}

task directDIA_search_binned {
    input {
        # Primary data inputs
        Array[File] input_files
        Int bin_index
        File fasta_1

        # Additional FASTA databases
        File? fasta_2
        File? fasta_3

        # Database & schema files
        File? enzyme_database
        File? analysis_settings

        # Resource parameters
        Int cpu
        Int ram_gb
        Float bin_size_gb
        Int disk_size_multiplier

        # Operational parameters
        Int n_preemptible = 0
    }

    command <<<
        set -euo pipefail

        cromwell_root=$(pwd)

        echo "Creating input directory..."
        input_dir="${cromwell_root}/work_input"
        mkdir -p "${input_dir}"

        echo "Creating output directory..."
        output_dir="${cromwell_root}/out_archive"
        mkdir -p "${output_dir}"

        echo "Creating temporary directory ..."
        tmp_dir="${cromwell_root}/sn_temp"
        mkdir -p "${tmp_dir}"

        # Copy all files to input directory
        echo "Copying files to the input directory..."
        while IFS= read -r htrms_file; do
            if [ -n "${htrms_file}" ]; then
                cp "${htrms_file}" "${input_dir}/"
            fi
        done < ~{write_lines(input_files)}

        # Import enzyme database if provided
        if [ ~{defined(enzyme_database)} = true ]; then
            echo "Importing the provided enzyme database..."
            dotnet /usr/lib/spectronaut/SpectronautCMD.dll --importEnzymeDB "~{enzyme_database}"
        fi

        echo "Starting directDIA search to generate search archive..."
        spectronaut direct \
            -d "${input_dir}" \
            -fasta "~{fasta_1}" \
            ~{if defined(fasta_2) then "-fasta " + fasta_2 else ""} \
            ~{if defined(fasta_3) then "-fasta " + fasta_3 else ""} \
            ~{if defined(analysis_settings) then "-s " + analysis_settings else ""} \
            -o "${output_dir}" \
            -setTemp "${tmp_dir}" 2>&1 | tee archive_generation.log

        # Rename and move .psar files with unique bin_index suffix
        echo "Moving search archives to the final output directory..."
        psar_count=$(find "${output_dir}" -type f -name "*.psar" | wc -l)
        if [ "${psar_count}" -eq 0 ]; then
            echo "ERROR: No .psar files produced" >&2
            exit 1
        fi

        find "${output_dir}" -type f -name "*.psar" | while read -r psar_file; do
            basename=$(basename "${psar_file}" .psar)
            new_name="${basename}_bin_~{bin_index}.psar"
            mv "${psar_file}" "${cromwell_root}/${new_name}"
        done

        echo "Archive generation complete!"

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
        Array[File] search_archives = glob("*.psar")
    }

    runtime {
        docker: "cameronlian/panoply-spectronaut:v20.3"
        cpu: cpu
        memory: "~{ram_gb}GB"
        bootDiskSizeGb: 128
        disks: "local-disk ~{ceil(bin_size_gb * disk_size_multiplier)} HDD"
        preemptible: n_preemptible
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
        disks: "local-disk 200 HDD"
        preemptible: 2
    }
}

task write_array_to_file {
    input {
        Array[String] file_paths
    }

    command <<<
        # Write file paths to output file
        while IFS= read -r file_path; do
            echo "${file_path}"
        done < ~{write_lines(file_paths)} > file_paths.txt
    >>>

    output {
        File output_file = "file_paths.txt"
    }

    runtime {
        docker: "google/cloud-sdk:slim"
        cpu: 2
        memory: "4GB"
        bootDiskSizeGb: 10
        disks: "local-disk 50 HDD"
        preemptible: 2
    }
}

task combine_archives {
    input {
        # Primary data inputs
        Array[File] input_archives

        # Resource parameters
        Int cpu
        Int ram_gb
        Float total_input_size_gb
        Int disk_size_multiplier

        # Operational parameters
        Int n_preemptible = 0
    }

    command <<<
        set -euo pipefail

        cromwell_root=$(pwd)
        merged_library="merged_library.kit"

        echo "Creating input directory..."
        work_archives="${cromwell_root}/work_archives"
        mkdir -p "${work_archives}"

        echo "Copying archives for merging..."
        while IFS= read -r archive; do
            if [ -n "${archive}" ]; then
                cp "${archive}" "${work_archives}/"
            fi
        done < ~{write_lines(input_archives)}

        echo "Merging search archives (.psar) into a library (.kit)..."
        spectronaut lg -se Pulsar \
            -sad "${work_archives}" \
            -k "${cromwell_root}/${merged_library}" \
            -o "${cromwell_root}" 2>&1 | tee merge_archives.log

        if [ ! -f "${cromwell_root}/${merged_library}" ]; then
            echo "ERROR: Merged archive file not found" >&2
            exit 1
        fi

        echo "Archive merging complete!"

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
        # Primary data inputs
        String experiment_name
        Array[File] input_files
        File search_archive
        Int bin_index
        File fasta_1

        # Additional FASTA databases
        File? fasta_2
        File? fasta_3

        # Database & schema files
        File? enzyme_database
        File? analysis_settings

        # Configuration files
        File? json_settings

        # Resource parameters
        Int cpu
        Int ram_gb
        Float bin_size_gb
        Int disk_size_multiplier

        # Operational parameters
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
        echo "Copying iles..."
        while IFS= read -r htrms_file; do
            if [ -n "${htrms_file}" ] && [ -f "${htrms_file}" ]; then
                cp "${htrms_file}" "${input_dir}/"
            fi
        done < ~{write_lines(input_files)}

                # Import enzyme database if provided
        if [ ~{defined(enzyme_database)} = true ]; then
            echo "Importing enzyme database..."
            dotnet /usr/lib/spectronaut/SpectronautCMD.dll --importEnzymeDB "~{
                enzyme_database}"
        fi

        spectronaut diaanalysis \
            ~{if defined(analysis_settings) then "-s " + analysis_settings else ""} \
            -fasta "~{fasta_1}" \
            ~{if defined(fasta_2) then "-fasta " + fasta_2 else ""} \
            ~{if defined(fasta_3) then "-fasta " + fasta_3 else ""} \
            ~{if defined(json_settings) then "-j " + json_settings else ""} \
            -n "~{experiment_name}_bin_~{bin_index}" \
            -o "${output_dir}" \
            -d "${input_dir}" \
            -a "~{search_archive}" \
            -setTemp "${tmp_dir}" 2>&1 | tee dia_analysis.log

        # Move all generated .sne files to output
        echo "Moving SNE files..."
        sne_count=$(find "${output_dir}" -type f -name "*.sne" | wc -l)
        if [ "${sne_count}" -eq 0 ]; then
            echo "ERROR: No .sne files found" >&2
            exit 1
        fi

        find "${output_dir}" -type f -name "*.sne" -exec mv {} "${cromwell_root}/" \;

        echo "DIA analysis complete. Generated ${sne_count} SNE files."

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
        Array[File] sne_files = glob("*.sne")
    }

    runtime {
        docker: "cameronlian/panoply-spectronaut:v20.3"
        cpu: cpu
        memory: "~{ram_gb}GB"
        bootDiskSizeGb: 128
        disks: "local-disk ~{ceil(bin_size_gb * disk_size_multiplier)} HDD"
        preemptible: n_preemptible
    }
}

task sne_merge {
    input {
        # Primary data inputs
        String experiment_name
        Array[File] sne_files

        # Configuration & schema files
        File? analysis_settings
        File? condition_setup

        # Report configuration
        File? report_schema_1
        File? report_schema_2
        File? report_schema_3
        File? report_schema_4

        # Resource parameters
        Int cpu
        Int ram_gb
        Float total_input_size_gb
        Int disk_size_multiplier

        # Operational parameters
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

        spectronaut manageSNE --merge \
            -n "~{experiment_name}_merged" \
            -o "${output_dir}" \
            -d "${sne_dir}" \
            ~{if defined(condition_setup) then "-con " + condition_setup else ""} \
            ~{if defined(analysis_settings) then "-s " + analysis_settings else ""} \
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

version development

# Archive Combination Module
# Combines search archives (.psar or .kit) from a provided directory into a single merged library

workflow parallel_spectronaut_archiveCombine {
    input {
        String archive_directory  # GCS directory containing .psar or .kit files
        Int disk_size_multiplier = 15  # Multiplier for disk size calculation
        String experiment_type = "proteome"  # proteome / ptm
    }

    # Compute preset configurations based on experiment_type
    Map[String, Int] combine_archives_cpu_presets = {
        "proteome": 16,
        "ptm": 16,
    }
    Map[String, Int] combine_archives_ram_gb_presets = {
        "proteome": 384,
        "ptm": 640
    }

    # Look up values based on experiment_type
    Int combine_archives_cpu = combine_archives_cpu_presets[experiment_type]
    Int combine_archives_ram_gb = combine_archives_ram_gb_presets[experiment_type]

    # List all archive files (.psar and .kit) in the provided directory
    call list_archive_files {
        input:
            gcs_path = archive_directory,
    }

    Array[String] archive_paths = read_lines(list_archive_files.archive_list)

    # Calculate total size of archives for disk allocation
    call calculate_archive_size {
        input:
            archive_paths = archive_paths,
    }

    # Combine archives into one merged library
    call combine_archives {
        input:
            archive_paths = archive_paths,
            total_input_size_gb = calculate_archive_size.total_size_gb,
            disk_size_multiplier = disk_size_multiplier,
            cpu = combine_archives_cpu,
            ram_gb = combine_archives_ram_gb,
    }

    output {
        File merged_library = combine_archives.merged_archive
    }
}

task list_archive_files {
    input {
        String gcs_path
    }

    command <<<
        set -euo pipefail

        raw_listing="raw_listing.txt"
        cleaned_listing="archive_list.txt"

        # Normalize the input path (remove trailing slashes)
        normalized_path=$(echo "~{gcs_path}" | sed 's:/*$::')

        gcloud storage ls "~{gcs_path}" > "${raw_listing}"

        # Filter for .psar and .kit files only, remove totals, trim trailing slashes, empty lines, filter out directory itself, unique
        grep -v 'TOTAL:' "${raw_listing}" | \
            sed 's:/*$::' | \
            sed '/^[[:space:]]*$/d' | \
            grep -v "^${normalized_path}$" | \
            grep -E '\.(psar|kit)$' | \
            sort -u > "${cleaned_listing}"

        # Check if we found any archives
        archive_count=$(wc -l < "${cleaned_listing}")
        if [ "${archive_count}" -eq 0 ]; then
            echo "ERROR: No .psar or .kit files found in ${normalized_path}" >&2
            exit 1
        fi

        echo "Found ${archive_count} archive files (.psar or .kit)"
    >>>

    output {
        File archive_list = "archive_list.txt"
    }

    runtime {
        docker: "google/cloud-sdk:slim"
        cpu: 2
        memory: "8GB"
        bootDiskSizeGb: 20
        disks: "local-disk 300 HDD"
    }
}

task calculate_archive_size {
    input {
        Array[String] archive_paths
    }

    command <<<
        set -euo pipefail

        total_bytes=0

        echo "Calculating total size of archive files..."
        while IFS= read -r archive_path; do
            if [ -n "${archive_path}" ]; then
                # Get size of each archive file
                size_bytes=$(gcloud storage du "${archive_path}" | awk '{print $1}')
                total_bytes=$((total_bytes + size_bytes))
                echo "Archive: ${archive_path} - Size: ${size_bytes} bytes"
            fi
        done < ~{write_lines(archive_paths)}

        # Convert to GB
        total_gb=$(awk "BEGIN {printf \"%.2f\", ${total_bytes} / (1024^3)}")
        echo "${total_gb}" > total_size_gb.txt
        echo "Total archive size: ${total_gb} GB"
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

task combine_archives {
    input {
        Array[String] archive_paths
        Float total_input_size_gb
        Int disk_size_multiplier
        Int cpu
        Int ram_gb
    }

    command <<<
        set -euo pipefail

        cromwell_root=$(pwd)
        merged_library="merged_library.kit"

        work_archives="${cromwell_root}/work_archives"
        mkdir -p "${work_archives}"

        # Download all archive files from GCS
        echo "Downloading archives from GCS..."
        while IFS= read -r archive_path; do
            if [ -n "${archive_path}" ]; then
                echo "Downloading: ${archive_path}"
                gcloud storage cp "${archive_path}" "${work_archives}/"
            fi
        done < ~{write_lines(archive_paths)}

        # Verify we have archives to combine
        archive_count=$(find "${work_archives}" -type f \( -name "*.psar" -o -name "*.kit" \) | wc -l)
        if [ "${archive_count}" -eq 0 ]; then
            echo "ERROR: No archive files downloaded" >&2
            exit 1
        fi

        echo "Combining ${archive_count} archive files..."

        # Combine archives using spectronaut lg command
        spectronaut lg -se Pulsar \
            -sad "${work_archives}" \
            -k "${cromwell_root}/${merged_library}" \
            -o "${cromwell_root}" 2>&1 | tee merge_archives.log

        if [ ! -f "${cromwell_root}/${merged_library}" ]; then
            echo "ERROR: Merged archive file not found" >&2
            exit 1
        fi

        echo "Archive merging complete."
    >>>

    output {
        File merged_archive = "merged_library.kit"
    }

    runtime {
        docker: "cameronlian/panoply-spectronaut:v20.0"
        cpu: cpu
        memory: "~{ram_gb}GB"
        bootDiskSizeGb: 128
        disks: "local-disk ~{ceil(total_input_size_gb * disk_size_multiplier)} HDD"
        preemptible: 0
    }
}

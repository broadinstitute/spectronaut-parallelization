version development

# Dynamic VM Binning with Size-based Disk Allocation
# Files are distributed across n VMs using round-robin binning
# Disk sizes are calculated dynamically based on actual input file sizes

workflow parallel_spectronaut {
    input {
        Int num_vms = 3  # Number of VMs to use for parallel processing
        Int disk_size_multiplier = 15  # Multiplier for disk size calculation
        Int conversion_disk_gb = 2000  # Fixed disk size for HTRMS conversion step

        File fasta_1
        String experiment_name
        String file_directory
        File? fasta_2
        File? fasta_3
        File? enzyme_database
        File? convert_schema
        File? directDIA_settings  # For search archive generation
        File? DIA_analysis_settings  # For the actual DIA search against combined search archive
        File? condition_setup
        File? report_schema_1
        File? report_schema_2
        File? report_schema_3
        File? report_schema_4
        File? json_settings
        String experiment_type = "proteome"  # proteome / ptm
        Int n_preemptible = 0
    }

    # Compute preset configurations based on experiment_type
    Map[String, Int] directDIA_search_cpu_presets = {
        "proteome": 80,
        "ptm": 128,
    }
    Map[String, Int] directDIA_search_ram_gb_presets = {
        "proteome": 256,
        "ptm": 512,
    }

    Map[String, Int] combine_archives_cpu_presets = {
        "proteome": 16,
        "ptm": 16,
    }
    Map[String, Int] combine_archives_ram_gb_presets = {
        "proteome": 384,
        "ptm": 640
    }

    Map[String, Int] dia_analysis_cpu_presets = {
        "proteome": 32,
        "ptm": 64,
    }
    Map[String, Int] dia_analysis_ram_gb_presets = {
        "proteome": 128,
        "ptm": 256,
    }

    Map[String, Int] combine_sne_cpu_presets = {
        "proteome": 16,
        "ptm": 16,
    }
    Map[String, Int] combine_sne_ram_gb_presets = {
        "proteome": 384,
        "ptm": 640,
    }

    # Look up values based on experiment_type
    Int directDIA_search_cpu = directDIA_search_cpu_presets[experiment_type]
    Int directDIA_search_ram_gb = directDIA_search_ram_gb_presets[experiment_type]

    Int combine_archives_cpu = combine_archives_cpu_presets[experiment_type]
    Int combine_archives_ram_gb = combine_archives_ram_gb_presets[experiment_type]

    Int dia_analysis_cpu = dia_analysis_cpu_presets[experiment_type]
    Int dia_analysis_ram_gb = dia_analysis_ram_gb_presets[experiment_type]

    Int combine_sne_cpu = combine_sne_cpu_presets[experiment_type]
    Int combine_sne_ram_gb = combine_sne_ram_gb_presets[experiment_type]

    # List all files in the provided directory
    call list_files {
        input:
            gcs_path = file_directory,
    }

    Array[String] file_paths = read_lines(list_files.file_list)

    # Create bins using round-robin distribution
    call create_bins {
        input:
            file_paths = file_paths,
            num_bins = num_vms,
    }

    Array[Array[String]] file_bins = read_json(create_bins.bins_json)

    # Scatter over bins - each bin is processed in one VM
    scatter (i in range(length(file_bins))) {
        # HTRMS conversion for all files in the bin
        call htrms_conversion_binned {
            input:
                input_file_paths = file_bins[i],
                convert_schema = convert_schema,
                conversion_disk_gb = conversion_disk_gb,
        }

        # DirectDIA search for all HTRMS files in the bin
        call directDIA_search_binned {
            input:
                input_files = htrms_conversion_binned.htrms_files,
                bin_size_gb = htrms_conversion_binned.total_size_gb,
                disk_size_multiplier = disk_size_multiplier,
                fasta_1 = fasta_1,
                fasta_2 = fasta_2,
                fasta_3 = fasta_3,
                analysis_schema = directDIA_settings,
                enzyme_database = enzyme_database,
                cpu = directDIA_search_cpu,
                ram_gb = directDIA_search_ram_gb,
                n_preemptible = n_preemptible,
        }
    }

    # Flatten arrays for combine operations
    Array[Array[File]] binned_htrms_files = htrms_conversion_binned.htrms_files
    Array[Float] bin_sizes = htrms_conversion_binned.total_size_gb
    Array[Array[File]] binned_archives = directDIA_search_binned.search_archives
    Array[File] all_archives = flatten(binned_archives)

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
    }

    # DIA analysis for each bin's HTRMS files
    scatter (i in range(length(file_bins))) {
        call dia_analysis_binned {
            input:
                input_files = binned_htrms_files[i],
                bin_size_gb = bin_sizes[i],
                disk_size_multiplier = disk_size_multiplier,
                search_archive = combine_archives.merged_archive,
                fasta_1 = fasta_1,
                fasta_2 = fasta_2,
                fasta_3 = fasta_3,
                analysis_schema = DIA_analysis_settings,
                json_settings = json_settings,
                cpu = dia_analysis_cpu,
                ram_gb = dia_analysis_ram_gb,
                bin_index = i,
                experiment_name = experiment_name,
                n_preemptible = n_preemptible,
        }
    }

    Array[File] all_sne = flatten(dia_analysis_binned.sne_files)

    # Sum all bin sizes for combine_sne disk allocation
    call sum_floats as sum_sne_sizes {
        input:
            values = bin_sizes,
    }

    # Combine scattered SNE files and generate reports
    call combine_sne {
        input:
            sne_files = all_sne,
            experiment_name = experiment_name,
            condition_setup = condition_setup,
            report_schema_1 = report_schema_1,
            report_schema_2 = report_schema_2,
            report_schema_3 = report_schema_3,
            report_schema_4 = report_schema_4,
            analysis_schema = DIA_analysis_settings,
            total_input_size_gb = sum_sne_sizes.total,
            disk_size_multiplier = disk_size_multiplier,
            ram_gb = combine_sne_ram_gb,
            cpu = combine_sne_cpu,
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
        Array[String] file_paths
        Int num_bins
    }

    command <<<
        python3 <<CODE
import json

# Read file paths
file_paths_file = "~{write_lines(file_paths)}"
with open(file_paths_file) as f:
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

task htrms_conversion_binned {
    input {
        Array[String] input_file_paths
        File? convert_schema
        Int conversion_disk_gb
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

        # Download all files in the bin
        echo "Downloading files from GCS..."
        while IFS= read -r file_path; do
            if [ -n "${file_path}" ]; then
                echo "Downloading: ${file_path}"
                gcloud storage cp -r "${file_path}" "${input_dir}/"
            fi
        done < ~{write_lines(input_file_paths)}

        # Convert all files to HTRMS
        echo "Starting HTRMS conversion..."
        spectronaut -convert \
            -i "${input_dir}" \
            -o "${output_dir}" \
            ~{if defined(convert_schema) then "-s " + convert_schema else ""} \
            -setTemp "${tmp_dir}" 2>&1 | tee htrms_conversion.log

        # Move all HTRMS files to output
        echo "Moving HTRMS files..."
        htrms_count=$(find "${output_dir}" -type f -name "*.htrms" | wc -l)
        if [ "${htrms_count}" -eq 0 ]; then
            echo "ERROR: No .htrms files produced" >&2
            exit 1
        fi

        find "${output_dir}" -type f -name "*.htrms" -exec mv {} "${cromwell_root}/" \;

        echo "Conversion complete. Generated ${htrms_count} HTRMS files."

        # Calculate total size in GB for HTRMS converted files
        echo "Calculating total HTRMS file size..."
        total_size_bytes=$(find "${cromwell_root}" -type f -name "*.htrms" -exec du -b {} + | awk '{sum += $1} END {print sum}')
        total_size_gb=$(awk "BEGIN {printf \"%.2f\", ${total_size_bytes} / (1024^3)}")
        echo "${total_size_gb}" > "${cromwell_root}/total_size_gb.txt"
        echo "Total HTRMS file size: ${total_size_gb} GB"
    >>>

    output {
        Array[File] htrms_files = glob("*.htrms")
        Float total_size_gb = read_float("total_size_gb.txt")
    }

    runtime {
        docker: "cameronlian/panoply-spectronaut:v20.0"
        cpu: 16
        memory: "32GB"
        bootDiskSizeGb: 128
        disks: "local-disk ~{conversion_disk_gb} HDD"
        preemptible: 0
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

        # Move all generated .psar files to output
        echo "Moving search archives..."
        psar_count=$(find "${output_dir}" -type f -name "*.psar" | wc -l)
        if [ "${psar_count}" -eq 0 ]; then
            echo "ERROR: No .psar files produced" >&2
            exit 1
        fi

        find "${output_dir}" -type f -name "*.psar" -exec mv {} "${cromwell_root}/" \;

        echo "Archive generation complete. Generated ${psar_count} search archives."
    >>>

    output {
        Array[File] search_archives = glob("*.psar")
    }

    runtime {
        docker: "cameronlian/panoply-spectronaut:v20.0"
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

        # Move all generated .sne files to output
        echo "Moving SNE files..."
        sne_count=$(find "${output_dir}" -type f -name "*.sne" | wc -l)
        if [ "${sne_count}" -eq 0 ]; then
            echo "ERROR: No .sne files found" >&2
            exit 1
        fi

        find "${output_dir}" -type f -name "*.sne" -exec mv {} "${cromwell_root}/" \;

        echo "DIA analysis complete. Generated ${sne_count} SNE files."
    >>>

    output {
        Array[File] sne_files = glob("*.sne")
    }

    runtime {
        docker: "cameronlian/panoply-spectronaut:v20.0"
        cpu: cpu
        memory: "~{ram_gb}GB"
        bootDiskSizeGb: 128
        disks: "local-disk ~{ceil(bin_size_gb * disk_size_multiplier)} HDD"
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
    >>>

    output {
        File spectronaut_output = "spectronaut_output.zip"
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

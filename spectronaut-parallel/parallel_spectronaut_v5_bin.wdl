version development

# No XIC file export for ALL STEPS ####
workflow parallel_spectronaut {
    input {
        File fasta_1
        # Boolean do_conversion = "true"
        # Boolean do_search = "true"
        String experiment_name
        String file_directory
        File? fasta_2
        File? fasta_3
        File? enzyme_database
        File? convert_schema
        File? directDIA_schema  # For search archive generation
        File? DIA_analysis_schema  # For the actual DIA search against combined search archive
        File? condition_setup
        File? report_schema_1
        File? report_schema_2
        File? report_schema_3
        File? report_schema_4
        File? json_settings
        String experiment_type = "proteome"  # proteome / ptm
        Int max_parallel_vms = 40  # Maximum number of parallel VMs for scatter operations (capped at 40)
    }

    # Fixed disk size configurations
    Int archive_generation_disk_gb = 1000
    Int search_disk_size_gb = 1000

    # Compute preset configurations based on experiment_type
    # Edit these Maps to add new experiment types or adjust CPU/memory for existing types
    # Format: Map[experiment_type -> value]
    # All CPU and RAM configurations are pooled here for easier adjustments

    Map[String, Int] directDIA_search_cpu_presets = {
        "proteome": 64,
        "ptm": 96,
    }
    Map[String, Int] directDIA_search_ram_gb_presets = {
        "proteome": 64,
        "ptm": 96,
    }

    Map[String, Int] combine_archives_cpu_presets = {
        "proteome": 16,
        "ptm": 16,
    }
    Map[String, Int] combine_archives_ram_gb_presets = {
        "proteome": 128,
        "ptm": 256,
    }

    Map[String, Int] dia_analysis_cpu_presets = {
        "proteome": 32,
        "ptm": 64,
    }
    Map[String, Int] dia_analysis_ram_gb_presets = {
        "proteome": 32,
        "ptm": 64,
    }

    Map[String, Int] combine_sne_cpu_presets = {
        "proteome": 16,
        "ptm": 16,
    }
    Map[String, Int] combine_sne_ram_gb_presets = {
        "proteome": 256,
        "ptm": 512,
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
    call list_files { input:
        gcs_path = file_directory,
    }

    Array[String] file_paths = read_lines(list_files.file_list)

    # Pre-evaluate optional inputs to avoid resolution issues in nested scatters
    File? opt_fasta_2 = fasta_2
    File? opt_fasta_3 = fasta_3
    File? opt_enzyme_database = enzyme_database
    File? opt_directDIA_schema = directDIA_schema
    File? opt_DIA_analysis_schema = DIA_analysis_schema
    File? opt_json_settings = json_settings

    # Batching logic to limit parallel VMs
    # Enforce maximum cap of 40 VMs regardless of user input
    Int capped_max_vms = if max_parallel_vms > 40 then 40 else max_parallel_vms

    # Create batches of file paths
    call chunk_strings { input:
        input_array = file_paths,
        max_chunks = capped_max_vms,
    }

    # Scatter over batches (max 40 VMs)
    scatter (batch in chunk_strings.chunked_arrays) {
        scatter (file_path in batch) {
            call htrms_conversion { input:
                input_file_path = file_path,
                convert_schema = convert_schema,
            }

            # directDIA search only to generate search archive
            # Pulsar search command does not support HTRMS files
            call directDIA_search { input:
                input_file = htrms_conversion.htrms_file,
                fasta_1 = fasta_1,
                fasta_2 = opt_fasta_2,
                fasta_3 = opt_fasta_3,
                analysis_schema = opt_directDIA_schema,
                enzyme_database = opt_enzyme_database,
                cpu = directDIA_search_cpu,
                ram_gb = directDIA_search_ram_gb,
                disk_gb = archive_generation_disk_gb,
            }
        }
    }

    # Flatten nested scatter outputs
    Array[File] all_search_archives = flatten(directDIA_search.search_archive)
    Array[File] all_htrms_files = flatten(htrms_conversion.htrms_file)

    # Combine scattered search archives into one
    call combine_archives { input:
        input_archives = all_search_archives,

        cpu = combine_archives_cpu,
        ram_gb = combine_archives_ram_gb,
    }

    # Batching logic for DIA analysis scatter
    call chunk_files { input:
        input_array = all_htrms_files,
        max_chunks = capped_max_vms,
    }

    # Search each file individually against the combined archive
    # Scatter over batches (max 40 VMs)
    scatter (htrms_batch in chunk_files.chunked_arrays) {
        scatter (htrms_file in htrms_batch) {
            call dia_analysis { input:
                input_file = htrms_file,
                search_archive = combine_archives.merged_archive,
                experiment_name = experiment_name,
                analysis_schema = opt_DIA_analysis_schema,
                fasta_1 = fasta_1,
                fasta_2 = opt_fasta_2,
                fasta_3 = opt_fasta_3,
                json_settings = opt_json_settings,
                disk_gb = search_disk_size_gb,
                cpu = dia_analysis_cpu,
                ram_gb = dia_analysis_ram_gb,
            }
        }
    }

    # Flatten nested scatter output
    Array[File] all_sne_files = flatten(dia_analysis.sne_file)

    # Calculate total size of all SNE files for dynamic disk sizing at combine_sne step
    call calculate_sne_total_size { input:
        sne_files = all_sne_files,
    }

    # Combine scattered SNE files and generate reports
    call combine_sne { input:
        sne_files = all_sne_files,
        experiment_name = experiment_name,
        condition_setup = condition_setup,
        report_schema_1 = report_schema_1,
        report_schema_2 = report_schema_2,
        report_schema_3 = report_schema_3,
        report_schema_4 = report_schema_4,
        analysis_schema = DIA_analysis_schema,

        ram_gb = combine_sne_ram_gb,
        cpu = combine_sne_cpu,
        disk_gb = calculate_sne_total_size.total_size_gb * 2,
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

        gcloud storage ls "~{gcs_path}" > "${raw_listing}"

        # Remove totals, trim trailing slashes and empty lines, unique
        grep -v 'TOTAL:' "${raw_listing}" | \
            sed 's:/*$::' | \
            sed '/^[[:space:]]*$/d' | \
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

task htrms_conversion {
    input {
        String input_file_path
        File? convert_schema
    }

    command <<<
        set -euo pipefail

        cromwell_root=$(pwd)
        input_dir="${cromwell_root}/work_input"
        mkdir -p "${input_dir}"

        output_dir="${cromwell_root}/out_conversion"
        mkdir -p "${output_dir}"

        # Dedicated temp directory to prevent root fs pressure and avoid OOM kills
        tmp_dir="${cromwell_root}/sn_temp"
        mkdir -p "${tmp_dir}"

        gcloud storage cp -r "~{input_file_path}" "${input_dir}/"

        spectronaut -convert \
            -i "${input_dir}" \
            -o "${output_dir}" \
            ~{if defined(convert_schema) then "-s " + convert_schema else ""} \
            -setTemp "${tmp_dir}" 2>&1 | tee htrms_conversion.log

        htrms_file=$(find "${output_dir}" -type f -name "*.htrms" -print -quit)

        if [ -z "${htrms_file}" ]; then
            echo "ERROR: No .htrms file produced" >&2
            exit 1
        fi

        # Extract basename and replace extension with .htrms
        input_basename=$(basename "~{input_file_path}")
        output_filename="${input_basename%.*}.htrms"

        mv "${htrms_file}" "${cromwell_root}/${output_filename}"
    >>>

    output {
        File htrms_file = "~{sub(basename(input_file_path), "\\.[^.]+$", "")}.htrms"
    }

    runtime {
        docker: "cameronlian/panoply-spectronaut:v20.0"
        cpu: 16
        memory: "32GB"
        bootDiskSizeGb: 128
        disks: "local-disk 500 HDD"
        preemptible: 0
    }
}

task directDIA_search {
    input {
        File fasta_1
        File input_file
        Int disk_gb
        Int cpu
        Int ram_gb
        File? analysis_schema
        File? fasta_2
        File? fasta_3
        File? enzyme_database
    }

    command <<<
        set -euo pipefail

        cromwell_root=$(pwd)
        input_dir="${cromwell_root}/work_input"
        mkdir -p "${input_dir}"

        output_dir="${cromwell_root}/out_archive"
        mkdir -p "${output_dir}"

        # Dedicated temp directory to prevent root fs pressure and avoid OOM kills
        tmp_dir="${cromwell_root}/sn_temp"
        mkdir -p "${tmp_dir}"

        # Copy localized File input to input directory
        cp "~{input_file}" "${input_dir}/"

        if [ ~{defined(enzyme_database)} = true ]; then
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

        search_archive=$(find "${output_dir}" -type f -name "*.psar" -print -quit)
        if [ -z "${search_archive}" ]; then
            echo "ERROR: No .psar file produced" >&2
            exit 1
        fi

        # Extract basename and replace extension with .psar
        input_basename=$(basename "~{input_file}")
        output_filename="${input_basename%.*}.psar"

        mv "${search_archive}" "${cromwell_root}/${output_filename}"
    >>>

    output {
        File search_archive = "~{sub(basename(input_file), "\\.[^.]+$", "")}.psar"
    }

    runtime {
        docker: "cameronlian/panoply-spectronaut:v20.0"
        cpu: cpu
        memory: "~{ram_gb}GB"
        bootDiskSizeGb: 128
        disks: "local-disk ~{disk_gb} HDD"
        preemptible: 0
    }
}

task combine_archives {
    input {
        Array[File] input_archives
        Int cpu
        Int ram_gb
    }

    command <<<
        set -euo pipefail

        cromwell_root=$(pwd)
        merged_library="merged_library.kit"

        work_archives="${cromwell_root}/work_archives"
        mkdir -p "${work_archives}"

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
    >>>

    output {
        File merged_archive = "merged_library.kit"
    }

    runtime {
        docker: "cameronlian/panoply-spectronaut:v20.0"
        cpu: cpu
        memory: "~{ram_gb}GB"
        bootDiskSizeGb: 128
        disks: "local-disk 2000 HDD"
        preemptible: 0
    }
}

task dia_analysis {
    input {
        File input_file
        File search_archive
        File fasta_1
        String experiment_name
        Int cpu
        Int ram_gb
        File? analysis_schema
        File? fasta_2
        File? fasta_3
        File? json_settings
        Int disk_gb = 2000
    }

    command <<<
        set -euo pipefail

        cromwell_root=$(pwd)

        input_dir="${cromwell_root}/work_input"
        mkdir -p "${input_dir}"

        cp "~{input_file}" "${input_dir}/"

        output_dir="${cromwell_root}/out_dia"
        mkdir -p "${output_dir}"

        tmp_dir="${cromwell_root}/work_dia_temp"
        mkdir -p "${tmp_dir}"

        # Generate a unique experiment name per raw file
        input_basename=$(basename "~{input_file}")
        file_basename="${input_basename%.*}"
        # unique_experiment_name="~{experiment_name}_${file_basename}"

        spectronaut diaanalysis \
            ~{if defined(analysis_schema) then " -s " + analysis_schema else ""} \
            ~{if defined(fasta_1) then " -fasta " + fasta_1 else ""} \
            ~{if defined(fasta_2) then " -fasta " + fasta_2 else ""} \
            ~{if defined(fasta_3) then " -fasta " + fasta_3 else ""} \
            ~{if defined(json_settings) then " -j " + json_settings else ""} \
            -n "${file_basename}" \
            -o "${output_dir}" \
            -d "${input_dir}" \
            -a "~{search_archive}" \
            -setTemp "${tmp_dir}" 2>&1 | tee dia_analysis.log

        sne_file=$(find "${output_dir}" -type f -name "*.sne" -print -quit)
        if [ -z "${sne_file}" ]; then
            echo "ERROR: No .sne file found" >&2
            exit 1
        fi

        mv "${sne_file}" "${cromwell_root}/${file_basename}.sne"
    >>>

    output {
        File sne_file = "~{sub(basename(input_file), "\\.[^.]+$", "")}.sne"
    }

    runtime {
        docker: "cameronlian/panoply-spectronaut:v20.0"
        cpu: cpu
        memory: "~{ram_gb}GB"
        bootDiskSizeGb: 128
        disks: "local-disk ~{disk_gb} HDD"
        preemptible: 0
    }
}

task calculate_sne_total_size {
    input {
        Array[File] sne_files
    }

    command <<<
        set -euo pipefail

        total_bytes=0

        # Fast batch processing: use stat in a single pass
        while IFS= read -r sne_file; do
            if [ -n "${sne_file}" ] && [ -f "${sne_file}" ]; then
                file_size=$(stat -c%s "${sne_file}")
                total_bytes=$((total_bytes + file_size))
            fi
        done < ~{write_lines(sne_files)}

        # Convert bytes to GB and round up
        # 1 GB = 1024^3 bytes = 1073741824 bytes
        total_gb=$(( (total_bytes + 1073741823) / 1073741824 ))

        # Ensure minimum of 1 GB
        if [ "${total_gb}" -lt 1 ]; then
            total_gb=1
        fi

        echo "${total_gb}" > total_size_gb.txt
        echo "Total SNE files size: ${total_bytes} bytes (${total_gb} GB)" >&2
    >>>

    output {
        Int total_size_gb = read_int("total_size_gb.txt")
    }

    runtime {
        docker: "google/cloud-sdk:slim"
        cpu: 2
        memory: "8GB"
        bootDiskSizeGb: 20
        disks: "local-disk 30 HDD"
    }
}

task combine_sne {
    input {
        Array[File] sne_files
        String experiment_name
        Int disk_gb
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

        while IFS= read -r sne_file; do
            if [ -n "${sne_file}" ]; then
                cp "${sne_file}" "${sne_dir}/"
            fi
        done < ~{write_lines(sne_files)}

        sne_count=$(find "${sne_dir}" -type f -name "*.sne" | wc -l)
        if [ "${sne_count}" -eq 0 ]; then
            echo "ERROR: No SNE files found" >&2
            exit 1
        fi

        # "spectronaut manageSNE --merge" merges SNE files on the experiment level, giving results equivalent to if the files were generated and run together 
        spectronaut manageSNE --merge \
            -n "~{experiment_name}_merged" \
            -o "${output_dir}" \
            -d "${sne_dir}" \
            -con "~{condition_setup}" \
            ~{if defined(analysis_schema) then "-s " + analysis_schema else ""} \
            ~{if defined(report_schema_1) then "-rs " + report_schema_1 else ""} \
            ~{if defined(report_schema_2) then "-rs " + report_schema_2 else ""} \
            ~{if defined(report_schema_3) then "-rs " + report_schema_3 else ""} \
            ~{if defined(report_schema_4) then "-rs " + report_schema_4 else ""} 2>&1 | tee spectronaut_combine.log

        zip -r "${output_zip}" "${output_dir}" -x \*.zip

        if [ ! -f "${output_zip}" ]; then
            echo "ERROR: Failed to create output zip file" >&2
            exit 1
        fi
    >>>

    output {
        File spectronaut_output = "spectronaut_output.zip"
    }

    runtime {
        docker: "cameronlian/panoply-spectronaut:v20.0"
        cpu: cpu
        memory: "~{ram_gb}GB"
        bootDiskSizeGb: 128
        disks: "local-disk ~{disk_gb} HDD"
        preemptible: 0
    }
}

task chunk_strings {
    input {
        Array[String] input_array
        Int max_chunks
    }

    command <<<
        python3 <<CODE
        import json
        import math

        # Read input array from WDL-generated JSON file
        with open('~{write_json(input_array)}', 'r') as f:
            input_array = json.load(f)

        max_chunks = ~{max_chunks}

        total_items = len(input_array)
        chunk_size = math.ceil(total_items / max_chunks)

        chunks = []
        for i in range(0, total_items, chunk_size):
            chunks.append(input_array[i:i + chunk_size])

        # Write chunks as JSON
        with open('chunked_arrays.json', 'w') as f:
            json.dump(chunks, f)
        CODE
    >>>

    output {
        Array[Array[String]] chunked_arrays = read_json("chunked_arrays.json")
    }

    runtime {
        docker: "python:3.9-slim"
        cpu: 2
        memory: "4GB"
        bootDiskSizeGb: 20
        disks: "local-disk 20 HDD"
    }
}

task chunk_files {
    input {
        Array[File] input_array
        Int max_chunks
    }

    command <<<
        python3 <<CODE
        import json
        import math

        # Read input array from WDL-generated JSON file
        with open('~{write_json(input_array)}', 'r') as f:
            input_array = json.load(f)

        max_chunks = ~{max_chunks}

        total_items = len(input_array)
        chunk_size = math.ceil(total_items / max_chunks)

        chunks = []
        for i in range(0, total_items, chunk_size):
            chunks.append(input_array[i:i + chunk_size])

        # Write chunks as JSON
        with open('chunked_arrays.json', 'w') as f:
            json.dump(chunks, f)
        CODE
    >>>

    output {
        Array[Array[File]] chunked_arrays = read_json("chunked_arrays.json")
    }

    runtime {
        docker: "python:3.9-slim"
        cpu: 2
        memory: "4GB"
        bootDiskSizeGb: 20
        disks: "local-disk 20 HDD"
    }
}

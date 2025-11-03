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
        Int n_preemptible = 0
    }

    # Fixed disk size configurations
    Int archive_generation_disk_gb = 1000
    Int search_disk_size_gb = 1000
    Int combine_sne_disk_gb = 1000

    # Compute preset configurations based on experiment_type
    # Edit these Maps to add new experiment types or adjust CPU/memory for existing types
    # Format: Map[experiment_type -> value]

    Map[String, Int] directDIA_search_cpu_presets = {
        "proteome": 80,
        "ptm": 128,
    }
    Map[String, Int] directDIA_search_ram_gb_presets = {
        "proteome": 256,
        "ptm": 512,
    }

    Map[String, Int] combine_archives_cpu_presets = {
        "proteome": 8,
        "ptm": 8,
    }
    Map[String, Int] combine_archives_ram_gb_presets = {
        "proteome": 256,
        "ptm": 512
    }

    Map[String, Int] dia_analysis_cpu_presets = {
        "proteome": 32,
        "ptm": 64,
    }
    Map[String, Int] dia_analysis_ram_gb_presets = {
        "proteome": 96,
        "ptm": 156,
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

    # Each input file gets converted and searched in their own VM
    scatter (file_path in file_paths) {
        call htrms_conversion { input:
            input_file_path = file_path,
            convert_schema = convert_schema,
        }

        # directDIA search only to generate search archive
        # Pulsar search command does not support HTRMS files
        call directDIA_search { input:
            input_file = htrms_conversion.htrms_file,
            fasta_1 = fasta_1,
            fasta_2 = fasta_2,
            fasta_3 = fasta_3,
            analysis_schema = directDIA_schema,
            enzyme_database = enzyme_database,

            cpu = directDIA_search_cpu,
            ram_gb = directDIA_search_ram_gb,
            disk_gb = archive_generation_disk_gb,
            n_preemptible = n_preemptible, 
        }
    }

    # Combine scattered search archives into one
    call combine_archives { input:
        input_archives = directDIA_search.search_archive,

        cpu = combine_archives_cpu,
        ram_gb = combine_archives_ram_gb,
    }

    # Search each file individually against the combined archive
    scatter (htrms_file in htrms_conversion.htrms_file) {
        call dia_analysis { input:
            input_file = htrms_file,
            search_archive = combine_archives.merged_archive,
            analysis_schema = DIA_analysis_schema,
            fasta_1 = fasta_1,
            fasta_2 = fasta_2,
            fasta_3 = fasta_3,
            json_settings = json_settings,

            disk_gb = search_disk_size_gb,
            cpu = dia_analysis_cpu,
            ram_gb = dia_analysis_ram_gb,
            n_preemptible = n_preemptible,
        }
    }

    # Combine scattered SNE files and generate reports
    call combine_sne { input:
        sne_files = dia_analysis.sne_file,
        experiment_name = experiment_name,
        condition_setup = condition_setup,
        report_schema_1 = report_schema_1,
        report_schema_2 = report_schema_2,
        report_schema_3 = report_schema_3,
        report_schema_4 = report_schema_4,
        analysis_schema = DIA_analysis_schema,

        ram_gb = combine_sne_ram_gb,
        cpu = combine_sne_cpu,
        disk_gb = combine_sne_disk_gb,
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
        Int n_preemptible = 0
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
        preemptible: n_preemptible
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
        Int cpu
        Int ram_gb
        File? analysis_schema
        File? fasta_2
        File? fasta_3
        File? json_settings
        Int disk_gb = 2000
        Int n_preemptible = 0
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

        spectronaut diaanalysis \
            ~{if defined(analysis_schema) then " -s " + analysis_schema else ""} \
            ~{if defined(fasta_1) then " -fasta " + fasta_1 else ""} \
            ~{if defined(fasta_2) then " -fasta " + fasta_2 else ""} \
            ~{if defined(fasta_3) then " -fasta " + fasta_3 else ""} \
            ~{if defined(json_settings) then " -j " + json_settings else ""} \
            -o "${output_dir}" \
            -d "${input_dir}" \
            -a "~{search_archive}" \
            -setTemp "${tmp_dir}" 2>&1 | tee dia_analysis.log

        sne_file=$(find "${output_dir}" -type f -name "*.sne" -print -quit)
        
        if [ -z "${sne_file}" ]; then
            echo "ERROR: No .sne file found" >&2
            exit 1
        fi
        
        # Generate a unique experiment name per raw file
        input_basename=$(basename "~{input_file}")
        output_filename="${input_basename%.*}.sne"

        mv "${sne_file}" "${cromwell_root}/${output_filename}"
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
        preemptible: n_preemptible
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

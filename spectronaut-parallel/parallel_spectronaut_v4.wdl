version development

workflow parallel_spectronaut {
    input {
        File fasta_1
        String file_directory
        String experiment_name
        File? fasta_2
        File? fasta_3
        File? analysis_schema
        File? enzyme_database
        File? condition_setup
        File? report_schema_1
        File? report_schema_2
        File? report_schema_3
        File? report_schema_4
        File? json_settings
        # File? search_archive
        Int archive_generation_disk_gb = 1000
        Int search_disk_size_gb = 1000
        Int sne_combine_disk_gb = 1000
        Int sne_combine_ram_gb = 256
    }

    call list_files { input:
        gcs_path = file_directory,
    }

    Array[File] file_paths = read_lines(list_files.file_list)

    scatter (file_path in file_paths) {
        call archive_generation { input:
            input_file_path = file_path,
            fasta_1 = fasta_1,
            fasta_2 = fasta_2,
            fasta_3 = fasta_3,
            analysis_schema = analysis_schema,
            enzyme_database = enzyme_database,
            disk_gb = archive_generation_disk_gb,
        }
    }

    call combine_archives { input:
        input_archives = archive_generation.search_archive,
    }

    scatter (file_path in file_paths) {
        call dia_analysis { input:
            input_file_path = file_path,
            search_archive = combine_archives.merged_archive,
            experiment_name = experiment_name,
            analysis_schema = analysis_schema,
            fasta_1 = fasta_1,
            fasta_2 = fasta_2,
            fasta_3 = fasta_3,
            json_settings = json_settings,

            disk_gb = search_disk_size_gb,
        }
    }

    call combine_sne { input:
        sne_files = dia_analysis.sne_file,
        experiment_name = experiment_name,
        fasta_1 = fasta_1,
        fasta_2 = fasta_2,
        fasta_3 = fasta_3,
        condition_setup = condition_setup,
        report_schema_1 = report_schema_1,
        report_schema_2 = report_schema_2,
        report_schema_3 = report_schema_3,
        report_schema_4 = report_schema_4,
        analysis_schema = analysis_schema,
        disk_gb = sne_combine_disk_gb,
        ram_gb = sne_combine_ram_gb,
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
        bootDiskSizeGb: 32
        disks: "local-disk 300 HDD"
    }
}

task archive_generation {
    input {
        File fasta_1
        String input_file_path
        Int disk_gb
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

        gcloud storage cp -r "~{input_file_path}" "${input_dir}/"

        if [ ~{defined(enzyme_database)} = true ]; then
            dotnet /usr/lib/spectronaut/SpectronautCMD.dll --importEnzymeDB "~{enzyme_database}"
        fi

        spectronaut lg -se Pulsar \
            -d "${input_dir}" \
            -fasta "~{fasta_1}" \
            ~{if defined(fasta_2) then "-fasta " + fasta_2 else ""} \
            ~{if defined(fasta_3) then "-fasta " + fasta_3 else ""} \
            ~{if defined(analysis_schema) then "-s " + analysis_schema else ""} \
            -o "${output_dir}" \
            -a "${output_dir}/search_archive.psar" \
            -setTemp "${tmp_dir}" 2>&1 | tee archive_generation.log

        search_archive=$(find "${output_dir}" -type f -name "*.psar" -print -quit)
        if [ -z "${search_archive}" ]; then
            echo "ERROR: No .psar file produced" >&2
            exit 1
        fi

        mv "${search_archive}" "${cromwell_root}/search_archive.psar"
    >>>

    output {
        File search_archive = "search_archive.psar"
    }

    runtime {
        docker: "cameronlian/panoply-spectronaut:v20.0"
        cpu: 96
        memory: "128GB"
        bootDiskSizeGb: 128
        disks: "local-disk ~{disk_gb} HDD"
        preemptible: 0
    }
}

task combine_archives {
    input {
        Array[File] input_archives
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
        cpu: 32
        memory: "256GB"  # Memory intensive - 896GB is the max allowed on N2D VMs
        bootDiskSizeGb: 128
        disks: "local-disk 2000 HDD"
        preemptible: 0
    }
}

task dia_analysis {
    input {
        File search_archive
        File fasta_1
        String input_file_path
        String experiment_name
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

        output_dir="${cromwell_root}/out_dia"
        mkdir -p "${output_dir}"

        tmp_dir="${cromwell_root}/work_dia_temp"
        mkdir -p "${tmp_dir}"

        gcloud storage cp -r "~{input_file_path}" "${input_dir}/"

        # Generate a unique experiment name per raw file 
        input_basename=$(basename "~{input_file_path}")
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
        File sne_file = "~{sub(basename(input_file_path), "\\.[^.]+$", "")}.sne"
    }

    runtime {
        docker: "cameronlian/panoply-spectronaut:v20.0"
        cpu: 48
        memory: "64GB"
        bootDiskSizeGb: 128
        disks: "local-disk ~{disk_gb} HDD"
        preemptible: 0
    }
}

task combine_sne {
    input {
        File fasta_1
        Array[File] sne_files
        String experiment_name
        File? fasta_2
        File? fasta_3
        File? condition_setup
        File? report_schema_1
        File? report_schema_2
        File? report_schema_3
        File? report_schema_4
        File? analysis_schema
        Int disk_gb = 2000
        Int ram_gb = 256
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
        cpu: 16
        memory: "~{ram_gb}GB"  # Max RAM allowed: 896GB
        bootDiskSizeGb: 128
        disks: "local-disk ~{disk_gb} HDD"
        preemptible: 0
    }
}

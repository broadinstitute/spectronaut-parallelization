version development

workflow panoply_spectronaut {
    input {
        File fasta  # Primary FASTA database file
        # Required inputs
        String file_directory  # GCS path to raw input files
        String experiment_name

        # Conversion settings
        File? convert_schema  # Optional schema for HTRMS conversion

        # Search settings
        File? analysis_settings
        File? condition_setup
        File? fasta_1  # Additional FASTA database
        File? enzyme_database

        # Spectral libraries - if none specified, perform DirectDIA
        File? spectral_library
        File? spectral_library_1

        # Report schemas
        File? report_schema
        File? report_schema_1
        File? report_schema_2
        File? json_settings

        # Workflow control
        Boolean do_conversion = true
        Boolean do_search = true
        Int n_preemptible_htrms_conversion = 2  # Preemptible attempts for conversion

        # Resource configuration
        Int num_cpus = 32
        Int ram_gb = 128
        Int local_disk_gb = 2000
    }

    # List all raw files in the input directory
    call list_files { input:
        gcs_path = file_directory,
    }

    Array[String] input_files = read_lines(list_files.file_list)

    if (do_conversion) {
        # Scatter over each individual file (one file per VM) for parallel conversion
        scatter (input_file in input_files) {
            call convert_htrms { input:
                input_file_path = input_file,
                convert_schema = convert_schema,
                n_preemptible = n_preemptible_htrms_conversion,
            }
        }
    }

    # Select converted files or original files for search
    Array[File] files_for_search = select_first([
        convert_htrms.htrms_file,
        input_files,
    ])

    if (do_search) {
        call spectronaut { input:
            experiment_name = experiment_name,
            input_files = files_for_search,
            fasta = fasta,
            analysis_settings = analysis_settings,
            condition_setup = condition_setup,
            fasta_1 = fasta_1,
            enzyme_database = enzyme_database,
            spectral_library = spectral_library,
            spectral_library_1 = spectral_library_1,
            report_schema = report_schema,
            report_schema_1 = report_schema_1,
            report_schema_2 = report_schema_2,
            json_settings = json_settings,
            num_cpus = num_cpus,
            ram_gb = ram_gb,
            local_disk_gb = local_disk_gb,
        }
    }

    output {
        File? spectronaut_output = spectronaut.spectronaut_output
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
        cpu: 4
        memory: "16GB"
        preemptible: 2
        bootDiskSizeGb: 20
        disks: "local-disk 50 HDD"
    }
}

task convert_htrms {
    meta {
        author: "C. Lian"
        email: "glian@broadinstitute.org"
    }

    input {
        String input_file_path  # GCS path to single input file
        Int n_preemptible
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

        echo "=== Converting HTRMS file: ~{input_file_path} ===" >&2

        gcloud storage cp -r "~{input_file_path}" "${input_dir}/"

        # Run HTRMS conversion
        spectronaut -convert \
            -i "${input_dir}" \
            -o "${output_dir}" \
            ~{if defined(convert_schema) then "-s " + convert_schema else ""} \
            -setTemp "${tmp_dir}" 2>&1 | tee htrms_conversion.log

        # Find and move the converted file
        htrms_file=$(find "${output_dir}" -type f -name "*.htrms" -print -quit)

        if [ -z "${htrms_file}" ]; then
            echo "ERROR: No .htrms file produced" >&2
            exit 1
        fi

        # Extract basename and rename
        input_basename=$(basename "~{input_file_path}")
        output_filename="${input_basename%.*}.htrms"

        mv "${htrms_file}" "${cromwell_root}/${output_filename}"
        echo "=== Converted to: ${output_filename} ===" >&2
    >>>

    output {
        File htrms_file = "~{sub(basename(input_file_path), "\\.[^.]+$", "")}.htrms"
    }

    runtime {
        docker: "broadcptacdev/panoply_spectronaut:v19.7"
        cpuPlatform: "AMD Rome"
        cpu: 16
        memory: "32GB"
        bootDiskSizeGb: 32
        disks: "local-disk 300 HDD"
        preemptible: n_preemptible
    }
}

task spectronaut {
    meta {
        author: "D. R. Mani, C. Lian"
        email: "proteogenomics@broadinstitute.org"
    }

    input {
        # Search databases
        File fasta
        Array[File] input_files  # Array of input files (converted HTRMS or raw)
        String experiment_name
        File? analysis_settings
        File? condition_setup
        File? fasta_1
        File? enzyme_database

        # Spectral libraries - if none specified, perform DirectDIA
        File? spectral_library
        File? spectral_library_1

        # Report schema
        File? report_schema
        File? report_schema_1
        File? report_schema_2
        File? json_settings
        Int num_cpus = 16
        Int ram_gb = 64
        Int local_disk_gb = 2000
    }

    command <<<
        set -euo pipefail

        echo "=== Spectronaut analysis task started (broadcptacdev/panoply_spectronaut:v19.7) ===" >&2
        echo "directDIA mode: ~{if !defined(spectral_library) then "true" else "false"}" >&2

        cromwell_root=$(pwd)
        out_zip="${cromwell_root}/spectronaut_output.zip"
        out_dir="${cromwell_root}/spectronaut_out"
        sn_temp="${cromwell_root}/sn_temp"
        input_dir="${cromwell_root}/data"

        mkdir -p "${out_dir}" "${sn_temp}" "${input_dir}"

        # Copy all input files to input directory
        echo "Copying input files to data directory..." >&2
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
        echo "Copied ${file_count} files to data directory" >&2

        echo "=== Running Spectronaut ===" >&2

        ~{if defined(enzyme_database) then "dotnet /usr/lib/spectronaut/SpectronautCMD.dll --importEnzymeDB "
            + enzyme_database else ""}

        spectronaut \
          ~{if !defined(spectral_library) then " direct" else ""} \
          ~{if defined(analysis_settings) then " -s " + analysis_settings else ""} \
          ~{if defined(condition_setup) then " -con " + condition_setup else ""} \
          ~{if defined(fasta_1) then " -fasta " + fasta_1 else ""} \
          ~{if defined(spectral_library) then " -a " + spectral_library else ""} \
          ~{if defined(spectral_library_1) then " -a " + spectral_library_1 else ""} \
          ~{if defined(report_schema) then " -rs " + report_schema else ""} \
          ~{if defined(report_schema_1) then " -rs " + report_schema_1 else ""} \
          ~{if defined(report_schema_2) then " -rs " + report_schema_2 else ""} \
          ~{if defined(json_settings) then " -j " + json_settings else ""} \
          -n ~{experiment_name} \
          -o "${out_dir}" \
          -fasta ~{fasta} \
          -d "${input_dir}" \
          -setTemp "${sn_temp}"

        zip -r "${out_zip}" "${out_dir}" -x \*.zip

        echo "=== Spectronaut search complete (broadcptacdev/panoply_spectronaut:v19.7) ===" >&2
    >>>

    output {
        File spectronaut_output = "spectronaut_output.zip"
    }

    runtime {
        docker: "broadcptacdev/panoply_spectronaut:v19.7"
        cpuPlatform: "AMD Rome"
        memory: "~{ram_gb}GB"  # 896GB max for AMD Rome
        bootDiskSizeGb: 32
        disks: "local-disk ~{local_disk_gb} HDD"
        preemptible: 0
        cpu: num_cpus
    }
}

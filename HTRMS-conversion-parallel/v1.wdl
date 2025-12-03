version development

workflow parallel_spectronaut {
    input {
        String file_directory
        File? convert_schema
        Int n_preemptible = 0
    }

    call list_files { input:
        gcs_path = file_directory,
    }

    Array[String] file_paths = read_lines(list_files.file_list)

    scatter (file_path in file_paths) {
        call htrms_conversion { input:
            input_file_path = file_path,
            convert_schema = convert_schema,
            n_preemptible = n_preemptible, 
        }
    }
}

task list_files {
    input {
        String gcs_path
    }

    command <<<
        set -euo pipefail 

        echo "Listing objects under: ~{gcs_path}" >&2

        raw_listing="raw_listing.txt"
        cleaned_listing="file_list.txt"

        gcloud storage ls "~{gcs_path}" > "${raw_listing}"

        # Clean up paths in the raw listing
        # Remove trailing slashes (for timsTOF .d files) and empty lines 
        grep -v 'TOTAL:' "${raw_listing}" | \
            sed 's:/*$::' | \
            sed '/^[[:space:]]*$/d' | \
            sort -u > "${cleaned_listing}"

        cat "${cleaned_listing}" >&2
    >>>

    output {
        File file_list = "file_list.txt"
    }

    runtime {
        docker: "google/cloud-sdk:slim"
        cpu: 2
        memory: "8GB"
        bootDiskSizeGb: 64
        disks: "local-disk 50 HDD"
    }
}

task htrms_conversion {
    input {
        String input_file_path
        File? convert_schema
        Int n_preemptible
    }

    command <<<
        set -euo pipefail
        cromwell_root=$(pwd)

        echo "=== HTRMS Conversion ====" >&2
        echo "Input file: ~{input_file_path}" >&2

        tmp_dir=$(mktemp -d tmp_input_XXXXXX)
        output_dir=$(mktemp -d tmp_output_XXXXXX)

        echo "Copying input file into workspace..." >&2
        gcloud storage cp -r "~{input_file_path}" "${tmp_dir}/"

        echo "Running HTRMS conversion..." >&2
        spectronaut -convert \
            -i "${tmp_dir}" \
            -o "${output_dir}" \
            ~{if defined(convert_schema) then "-s " + convert_schema else ""} 2>&1 | tee spectronaut_convert.log

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
        docker: "broadcptacdev/panoply_spectronaut:v20.3"
        cpu: 16
        memory: "32GB"
        bootDiskSizeGb: 128
        disks: "local-disk 500 HDD"
        preemptible: n_preemptible
    }
}
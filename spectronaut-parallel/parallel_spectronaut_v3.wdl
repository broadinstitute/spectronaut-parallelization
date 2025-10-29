version development

workflow parallel_spectronaut {
    input {
        File fasta_1
        String file_directory
        String experiment_name
        File? convert_schema
        File? fasta_2
        File? fasta_3
        File? analysis_schema
        File? enzyme_database
        File? report_schema_1
        File? report_schema_2
        File? report_schema_3
        File? report_schema_4
        File? json_settings
        Boolean do_conversion = true
        Boolean do_search = true

        # Compute resource configurations
        Int archive_generation_disk_gb = 1000
        Int search_disk_size_gb = 2000
        Int sne_combine_disk_gb = 2000
    }

    call list_files { input:
        gcs_path = file_directory,
    }

    Array[File] file_paths = read_lines(list_files.file_list)

    if (do_conversion && do_search) {
        scatter (file_path in file_paths) {
            call htrms_conversion { input:
                input_file_path = file_path,
                convert_schema = convert_schema,
            }

            call archive_generation { input:
                input_file = htrms_conversion.htrms_file,
                fasta_1 = fasta_1,
                fasta_2 = fasta_2,
                fasta_3 = fasta_3,
                enzyme_database = enzyme_database,
                disk_gb = archive_generation_disk_gb,
            }
        }

        call combine_archives { input:
            input_archives = archive_generation.search_archive,
        }

        scatter (htrms_file in htrms_conversion.htrms_file) {
            call dia_analysis { input:
                input_file = htrms_file, 
                search_archive = combine_archives.merged_archive,
                experiment_name = experiment_name,
                analysis_schema = analysis_schema,
                fasta_1 = fasta_1,
                fasta_2 = fasta_2,
                fasta_3 = fasta_3,
                json_settings = json_settings,
                
                disk_gb = search_disk_size_gb
            }
        }

        call combine_sne { input:
            input_snes = dia_analysis.sne_file,
            experiment_name = experiment_name,
            fasta_1 = fasta_1,
            fasta_2 = fasta_2,
            fasta_3 = fasta_3,
            report_schema_1 = report_schema_1,
            report_schema_2 = report_schema_2,
            report_schema_3 = report_schema_3,
            report_schema_4 = report_schema_4,
            analysis_schema = analysis_schema,
            disk_gb = sne_combine_disk_gb,
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
        cpu: 8
        memory: "16GB"
        bootDiskSizeGb: 64
        disks: "local-disk 300 SSD"
    }
}

task htrms_conversion {
    input {
        String input_file_path
        File? convert_schema
    }

    command <<<
        set -euo pipefail 

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

        find "${output_dir}" -type f -name "*.htrms" -print -quit > converted.txt
        cat converted.txt >&2
    >>>

    output {
        File htrms_file = read_string("converted.txt")
    }

    runtime {
        docker: "cameronlian/panoply-spectronaut:v20.0"
        cpu: 16
        memory: "32GB"
        bootDiskSizeGb: 128
        disks: "local-disk 1000 SSD"
        preemptible: 0
    }
}

task archive_generation {
    input {
        File input_file
        Int disk_gb
        File? fasta_1
        File? fasta_2
        File? fasta_3
        File? enzyme_database
    }

    command <<<
        set -euo pipefail 

        echo "=== Spectronaut Search Archive Generation ===" >&2
        echo "Input file: ~{input_file}" >&2

        if [ ~{defined(enzyme_database)} = true ]; then 
            echo "Importing enzyme database..." >&2
            dotnet SpectronautCMD.dll --importEnzymeDB "~{enzyme_database}"
        fi 

        output_dir=$(mktemp -d archive_output_XXXXXXX)

        echo "Generating search archive..." >&2
        spectronaut lg -se Pulsar \
            -r "~{input_file}" \
            -fasta "~{fasta_1}" \
            ~{if defined(fasta_2) then "-fasta " + fasta_2 else ""} \
            ~{if defined(fasta_3) then "-fasta " + fasta_3 else ""} \
            -a "${output_dir}" \
            -k "${output_dir}" 2>&1 | tee archive_generation.log

        find "${output_dir}" -type f -name "*.psar" -print -quit > search_archive.txt
    >>>

    output {
        File search_archive = read_string("search_archive.txt")
    }

    runtime {
        docker: "cameronlian/panoply-spectronaut:v20.0"
        cpu: 128
        memory: "256GB"
        bootDiskSizeGb: 128
        disks: "local-disk ~{disk_gb} SSD"
        preemptible: 0
    }
}

task combine_archives {
    input {
        Array[File] input_archives
    }

    command <<<
                set -euo pipefail
                
                echo "=== Merging search archives ===" >&2
                echo "Number of input archives: ~{length(input_archives)}" >&2

                output_dir=$(mktemp -d output_XXXXXX)
                merged_library="merged_library.kit"

                python3 <<CODE
        import json
        from pathlib import Path 

        archives = json.loads(r'''~{write_json(input_archives)}''')
        Path("archive_list.txt").write_text(
            "\n".join(archives) + ("\n" if archives else ""),
            encoding="utf-8",
        )
        CODE

                echo "Validating input archives..." >&2
                while IFS= read -r archive; do
                    if [ -z "${archive}" ]; then
                        continue
                    fi
                    if [ ! -f "${archive}" ]; then
                        echo "ERROR: Missing archive file: ${archive}" >&2
                        exit 1
                    fi
                    echo "  ✓ $(basename "${archive}")" >&2
                done < archive_list.txt

                # Build -sa arguments
                sa_args=""
                while IFS= read -r archive; do
                    [ -z "${archive}" ] && continue
                    sa_args="${sa_args} -sa \"${archive}\""
                done < archive_list.txt

                echo "Merging search archives..." >&2
                # shellcheck disable=SC2086
                eval spectronaut lg -se Pulsar \
                    ${sa_args} \
                    -k "${merged_library}" \
                    -o "${output_dir}" 2>&1 | tee merge_archives.log

                find "${output_dir}" -type f -name "${merged_library}" -print -quit > merged_archive.txt
                cat merged_archive.txt >&2
    >>>

    output {
        File merged_archive = read_string("merged_archive.txt")
    }

    runtime {
        docker: "cameronlian/panoply-spectronaut:v20.0"
        cpu: 32
        memory: "512GB" # Memory intensive - 896GB is the max allowed on N2D VMs
        bootDiskSizeGb: 128
        disks: "local-disk 2000 SSD"
        preemptible: 0
    }
}

task dia_analysis {
    input {
        File input_file 
        File search_archive
        String experiment_name
        File? analysis_schema
        File fasta_1
        File? fasta_2
        File? fasta_3
        File? json_settings
        Int disk_gb = 2000
    }

    command <<<
        set -euo pipefail 

        echo "=== Spectronaut DIA Analysis ===" >&2
        echo "Input file: ~{input_file}" >&2
        echo "Search archive: ~{search_archive}" >&2

        output_dir=$(mktemp -d output_XXXXXX)
        tmp_dir=$(mktemp -d tmp_XXXXXX)
        
        # Generate unique experiment name to avoid .sne file collisions
        input_basename=$(basename "~{input_file}")
        file_basename="${input_basename%.*}"  # Remove extension
        unique_experiment_name="~{experiment_name}_${file_basename}"

        echo "Unique experiment name per file: ${unique_experiment_name}" >&2

        echo "Running DIA analysis..." >&2
        spectronaut diaanalysis \
            ~{if defined(analysis_schema) then " -s " + analysis_schema else ""} \
            ~{if defined(fasta_1) then " -fasta " + fasta_1 else ""} \
            ~{if defined(fasta_2) then " -fasta " + fasta_2 else ""} \
            ~{if defined(fasta_3) then " -fasta " + fasta_3 else ""} \
            ~{if defined(json_settings) then " -j " + json_settings else ""} \
            -n "${unique_experiment_name}" \
            -o "${output_dir}" \
            -r "~{input_file}" \
            -a "~{search_archive}" \
            -setTemp "${tmp_dir}" 2>&1 | tee dia_analysis.log

        find "${output_dir}" -type f -name "*.sne" -print -quit > sne_file.txt
        cat sne_file.txt >&2
    >>>

    output {
        File sne_file = read_string("sne_file.txt")
    }

    runtime {
        docker: "cameronlian/panoply-spectronaut:v20.0"
        cpu: 128 # CPU intensive 
        memory: "256GB"
        bootDiskSizeGb: 128
        disks: "local-disk ~{disk_gb} SSD"
        preemptible: 0
    }
}

task combine_sne {
    input {
        Array[File] input_snes
        String experiment_name
        File fasta_1
        File? fasta_2
        File? fasta_3
        File? report_schema_1
        File? report_schema_2
        File? report_schema_3
        File? report_schema_4
        File? analysis_schema
        Int disk_gb = 2000
    }

    command <<<
        set -euo pipefail 

        echo "=== SNE Combine ===" >&2
        echo "Experiment name: ~{experiment_name}" >&2
        echo "Number of input SNE files: ~{length(input_snes)}" >&2

        output_dir="sne_combine_output"
        output_zip="spectronaut_output.zip"

        mkdir -p "${output_dir}"

        python3 <<CODE
        import json
        from pathlib import Path

        sne_files = json.loads(r'''~{write_json(input_snes)}''')
        Path("sne_inputs.txt").write_text(
            "\n".join(sne_files) + ("\n" if sne_files else ""),
            encoding="utf-8",
        )
        CODE

        echo "Validating input files..." >&2
                while IFS= read -r sne; do
                    if [ -z "${sne}" ]; then
                        continue
                    fi
                    if [ ! -f "${sne}" ]; then
                        echo "ERROR: Missing SNE file: ${sne}" >&2
                        exit 1
                    fi
                    echo "  ✓ $(basename "${sne}")" >&2
                done < sne_inputs.txt

                # Build -sne arguments
                sne_args=""
                while IFS= read -r sne; do
                    [ -z "${sne}" ] && continue
                    sne_args="${sne_args} -sne \"${sne}\""
                done < sne_inputs.txt

                echo "Combining SNE files..." >&2
                # shellcheck disable=SC2086
                eval spectronaut combine \
                    -n "~{experiment_name}" \
                    -o "${output_dir}" \
                    ${sne_args} \
                    -fasta "~{fasta_1}" \
                    ~{if defined(fasta_2) then "-fasta " + fasta_2 else ""} \
                    ~{if defined(fasta_3) then "-fasta " + fasta_3 else ""} \
                    ~{if defined(analysis_schema) then "-s " + analysis_schema else ""} \
                    ~{if defined(report_schema_1) then "-rs " + report_schema_1 else ""} \
                    ~{if defined(report_schema_2) then "-rs " + report_schema_2 else ""} \
                    ~{if defined(report_schema_3) then "-rs " + report_schema_3 else ""} \
                    ~{if defined(report_schema_4) then "-rs " + report_schema_4 else ""} 2>&1 | tee spectronaut_combine.log
                
                echo "SNE combine complete; creating output zip..." >&2
                zip -r "${output_zip}" "${output_dir}" -x \*.zip
                mv "${output_zip}" "/$(pwd)/"
    >>>

    output {
        File spectronaut_output = "spectronaut_output.zip"
    }
    
    runtime {
        docker: "cameronlian/panoply-spectronaut:v20.0"
        cpu: 32
        memory: "512GB"
        bootDiskSizeGb: 128
        disks: "local-disk ~{disk_gb} SSD"
        preemptible: 0
    }
}
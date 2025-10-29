version development

workflow parallel_spectronaut {
    input {
        Boolean do_conversion = true
        Boolean do_search = true
        String file_directory
        String experiment_name # Required when do_search=true
        File? convert_schema
        File fasta_1 # Required when do_search=true
        File? fasta_2
        File? fasta_3
        File? analysis_schema # Contains schema for Pulsar search and library generation steps 
        File? enzyme_database
        # File? spectral_library_1
        # File? spectral_library_2
        File? report_schema_1
        File? report_schema_2
        File? report_schema_3
        File? report_schema_4
        File? json_settings
        Int archive_generation_disk_gb = 1000 
        Int search_disk_size_gb = 2000 # If files are stored in GCS bucket, they will be first downloaded to the local disk for processing, so larger disk size may be needed 
        Int sne_combine_disk_gb = 2000
    }

    # List all files under the provided input file directory
    call list_files { input:
        gcs_path = file_directory,
    }

    Array[File] file_paths = read_lines(list_files.file_list)

    # Spectronaut Search
    # Step 1: Search archive generation - low RAM, high CPU --> scatter
    # Step 2: Merge search archives - high RAM, low CPU
    # Step 3: DIA analysis - low RAM, high CPU --> scatter
    # Step 4: Merge SNE files - high RAM, low CPU

    # Scenario 1: do_conversion=true
    if (do_conversion && do_search) {
        # Input files scatter to generate HTRMS files
        scatter (file_path in file_paths) {
            call htrms_conversion { input:
                input_file_path = file_path,
                convert_schema = convert_schema,
            }
        }

        # HTRMS files are scattered to generate search archives
        scatter (htrms_file in htrms_conversion.htrms_files) {
            call archive_generation { input:
                input_file = htrms_file,
                # experiment_name = experiment_name,
                fasta_1 = fasta_1,
                fasta_2 = fasta_2,
                fasta_3 = fasta_3,
                analysis_schema = analysis_schema,
                enzyme_database = enzyme_database,
                disk_gb = archive_generation_disk_gb,
            }
        }

        # Individual search archives are combined into one search archive
        # e.g., spectronaut lg -se Pulsar -sa intermediate/search_archive1.psar -sa intermediate/search_archive2.psar ... -k intermediate/output_library.kit -o intermediate
        call merge_archives { input:
            input_archives = archive_generation.search_archives,
        }

        # HTRMS files are scattered to perform DIA analysis using the combined search archive
        # e.g., spectronaut diaanalysis -o intermediate -a intermediate/output_library.kit -d raw -fasta fasta/20210709-SP_mouse_isoforms_UP000000589.fasta -fasta fasta/crap.2009.05.01.fasta -n "PXD032759"
        scatter (htrms_file in htrms_conversion.htrms_files) {
            call dia_analysis { input:
                input_file = htrms_file,
                analysis_schema = analysis_schema,
                search_archive = merge_archives.merged_archive,
                fasta_1 = fasta_1,
                fasta_2 = fasta_2,
                fasta_3 = fasta_3,
                experiment_name = experiment_name,
                json_settings = json_settings,
                disk_gb = search_disk_size_gb,
            }
        }

        # Individual SNE files are combined into one final SNE file with reports generated
        # e.g., spectronaut manageSNE --merge -o results -sne intermediate/<subfolder1>/<timestamp>_PXD032759.sne -sne intermediate/<subfolder2>/<timestamp>_PXD032759.sne ... -o results
        call sne_combine { input:
            input_sne = dia_analysis.sne_files,
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

    # # Scenario 2: do_conversion=false + file_directory (either unconverted raw files or HTRMS files)
    # if (!do_conversion && do_search) {
    #     # Input files are scattered to generate search archives
    #     scatter (input_file in file_paths) {
    #         call archive_generation as archive_generation_s2 { input:
    #             input_file = input_file,
    #             fasta_1 = fasta_1,
    #             fasta_2 = fasta_2,
    #             fasta_3 = fasta_3,
    #             analysis_schema = analysis_schema,
    #             enzyme_database = enzyme_database,
    #             disk_gb = archive_generation_disk_gb,
    #         }
    #     }

    #     # Individual search archives are combined into one search archive
    #     # e.g., spectronaut lg -se Pulsar -sa intermediate/search_archive1.psar -sa intermediate/search_archive2.psar ... -k intermediate/output_library.kit -o intermediate
    #     call merge_archives as merge_archives_s2 { input:
    #         input_archives = archive_generation_s2.search_archives,
    #     }

    #     # HTRMS files are scattered to perform DIA analysis using the combined search archive
    #     # e.g., spectronaut diaanalysis -o intermediate -a intermediate/output_library.kit -d raw -fasta fasta/20210709-SP_mouse_isoforms_UP000000589.fasta -fasta fasta/crap.2009.05.01.fasta -n "PXD032759"
    #     scatter (data_file in file_paths) {
    #         call dia_analysis as dia_analysis_s2 { input:
    #             input_file = data_file,
    #             search_archive = merge_archives_s2.merged_archive,
    #             analysis_schema = analysis_schema,
    #             fasta_1 = fasta_1,
    #             fasta_2 = fasta_2,
    #             fasta_3 = fasta_3,
    #             experiment_name = experiment_name,
    #             json_settings = json_settings,
    #             disk_gb = search_disk_size_gb,
    #         }
    #     }

    #     # Individual SNE files are combined into one final SNE file with reports generated
    #     # e.g., spectronaut manageSNE --merge -o results -sne intermediate/<subfolder1>/<timestamp>_PXD032759.sne -sne intermediate/<subfolder2>/<timestamp>_PXD032759.sne ... -o results
    #     call sne_combine as sne_combine_s2 { input:
    #         input_sne = dia_analysis_s2.sne_files,
    #         experiment_name = experiment_name,
    #         fasta_1 = fasta_1,
    #         fasta_2 = fasta_2,
    #         fasta_3 = fasta_3,
    #         report_schema_1 = report_schema_1,
    #         report_schema_2 = report_schema_2,
    #         report_schema_3 = report_schema_3,
    #         report_schema_4 = report_schema_4,
    #         analysis_schema = analysis_schema,
    #         disk_gb = sne_combine_disk_gb,
    #     }
    # }

    output {
        File discovered_inputs = list_files.file_list
        Array[File]? converted_htrms = htrms_conversion.htrms_files
        Array[File]? search_archives = if do_search then select_first([
            archive_generation.search_archives,
            archive_generation_s2.search_archives,
        ]) else None
        File? merged_library = if do_search then select_first([
            merge_archives.merged_archive,
            merge_archives_s2.merged_archive,
        ]) else None
        Array[File]? individual_sne_files = if do_search then select_first([
            dia_analysis.sne_files,
            dia_analysis_s2.sne_files,
        ]) else None
        File? combined_output = if do_search then select_first([
            sne_combine.spectronaut_output,
            sne_combine_s2.spectronaut_output,
        ]) else None
    }
}

task list_files {
    input {
        String gcs_path
    }

    command <<<
        set -euo pipefail

        echo "Listing objects under: ~{gcs_path}" >&2

        # Validate the supplied string is a GCS path with "gs://" prefix
        if [[ ! "~{gcs_path}" =~ ^gs:// ]]; then
            echo "ERROR: Input must be a GCS path (gs://...): ~{gcs_path}" >&2
            exit 1
        fi

        raw_listing="raw_listing.txt"
        cleaned_listing="file_list.txt"

        # This step requires GCloud CLI included in the Docker image
        if ! gcloud storage ls "~{gcs_path}" > "${raw_listing}"; then
            echo "ERROR: Failed to list objects at: ~{gcs_path}" >&2
            exit 1
        fi

        # Clean up paths in the input file manifest: remove trailing slashes (for timsTOF .d files) and empty lines
        if ! grep -v 'TOTAL:' "${raw_listing}" | \
            sed 's:/*$::' | \
            sed '/^[[:space:]]*$/d' | \
            sort -u > "${cleaned_listing}"; then
            echo "ERROR: Failed to clean listing output" >&2
            exit 1
        fi

        echo "Discovered entries:" >&2
        cat "${cleaned_listing}" >&2

        if [ ! -s "${cleaned_listing}" ]; then
            echo "ERROR: No files or folders found under: ~{gcs_path}" >&2
            exit 1
        fi
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

        echo "=== Spectronaut HTRMS conversion ===" >&2
        echo "Source object: ~{input_file_path}" >&2

        tmp_root=$(mktemp -d tmp_input_XXXXXX)
        output_dir=$(mktemp -d tmp_output_XXXXXX)

        echo "Temporary input directory: ${tmp_root}" >&2
        echo "Temporary output directory: ${output_dir}" >&2

        # For timsTOF files, the entire .d directory needs to be copied recursively, and its parent directory will be used as input to the conversion
        # Otherwise risking not finding the input file to convert when the input file is a directory
        echo "Copying source object into workspace..." >&2
        if ! gcloud storage cp -r "~{input_file_path}" "${tmp_root}/"; then
            echo "ERROR: Failed to copy object: ~{input_file_path}" >&2
            rm -rf "${tmp_root}" "${output_dir}"
            exit 1
        fi

        # Validate if the input file has been copied to the temporary directory
        echo "Input directory contents:" >&2
        ls -lhR "${tmp_root}" >&2

        # The output should have the same base name as the input, but with .htrms extension
        input_basename=$(basename "~{input_file_path}")
        expected_basename=$(echo "${input_basename}" | sed -E 's/\.(raw|d|RAW|D)$//')
        if [ -z "${expected_basename}" ]; then
            expected_basename="${input_basename}"
        fi
        expected_output="${expected_basename}.htrms"

        echo "Running Spectronaut conversion for ${input_basename}" >&2
        spectronaut -convert \
            -i "${tmp_root}" \
            -o "${output_dir}" \
            ~{if defined(convert_schema) then "-s " + convert_schema else ""} 2>&1 | tee spectronaut_convert.log

        convert_status=${PIPESTATUS[0]}
        if [ "${convert_status}" -ne 0 ]; then
            echo "ERROR: Spectronaut conversion failed (exit ${convert_status})" >&2
            rm -rf "${tmp_root}" "${output_dir}"
            exit "${convert_status}"
        fi

        echo "Conversion finished; locating HTRMS output" >&2
        converted_file=$(find "${output_dir}" -type f -name "*.htrms" | head -n 1)
        if [ -z "${converted_file}" ]; then
            echo "ERROR: No .htrms file was produced" >&2
            find "${output_dir}" -type f >&2
            rm -rf "${tmp_root}" "${output_dir}"
            exit 1
        fi

        # Copying to expected_output ensures the file is in the current working directory with a matching filename so that Cromwell can properly delocalize it to the execution bucket 
        echo "Found converted file: ${converted_file}" >&2
        if ! cp "${converted_file}" "${expected_output}"; then
            echo "ERROR: Failed to copy converted output" >&2
            rm -rf "${tmp_root}" "${output_dir}"
            exit 1
        fi

        echo "Cleaning up raw input clones to conserve disk space" >&2
        rm -rf "${tmp_root}" "${output_dir}"

        echo "Produced HTRMS file: ${expected_output}" >&2
        ls -lh "${expected_output}" >&2
    >>>

    output {
        File htrms_files = sub(basename(input_file_path), "\\.(raw|d|RAW|D)$", "") + ".htrms"
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
        File input_file # Cromwell automatically localizes File inputs to the task execution directory
        File fasta_1
        File? fasta_2
        File? fasta_3
        File? analysis_schema # TEST if individual Pulsar search and library generation schemas are required
        File? enzyme_database
        # String experiment_name # Without providing experiment_name in the spectronaut command, spectronaut will use the input file name as the experiment name by default
        Int disk_gb = 1000
    }

    command <<<
        set -euo pipefail

        echo "=== Spectronaut search archive generation ===" >&2
        echo "Input file: ~{input_file}" >&2

        data_dir=$(mktemp -d data_XXXXXX)

        # Copy input file to data directory (handles both regular files and timsTOF .d directories)
        echo "Copying input file to data directory..." >&2
        if ! cp -r "~{input_file}" "${data_dir}/"; then
            echo "ERROR: Failed to copy input file" >&2
            rm -rf "${data_dir}"
            exit 1
        fi

        echo "Data directory contents:" >&2
        ls -lh "${data_dir}" >&2

        # Import enzyme database if provided
        if [ ~{defined(enzyme_database)} = true ]; then
            echo "Importing enzyme database..." >&2
            dotnet /usr/lib/spectronaut/SpectronautCMD.dll --importEnzymeDB "~{enzyme_database}"
        fi

        # Generate unique archive name based on input file
        input_basename=$(basename "~{input_file}")
        # Remove known file extensions to match WDL output block regex
        archive_basename=$(echo "${input_basename}" | sed -E 's/\.(htrms|raw|d|HTRMS|RAW|D)$//')
        archive_output="${archive_basename}.psar"
        library_output="${archive_basename}.kit"

        echo "Generating search archive: ${archive_output}" >&2
        spectronaut lg -se Pulsar \
            -d "${data_dir}" \
            -fasta "~{fasta_1}" \
            ~{if defined(fasta_2) then "-fasta " + fasta_2 else ""} \
            ~{if defined(fasta_3) then "-fasta " + fasta_3 else ""} \
            ~{if defined(analysis_schema) then "-s " + analysis_schema else ""} \
            -a "${archive_output}" \
            -k "${library_output}" 2>&1 | tee archive_generation.log
        # Output both search archive and spectral library 

        archive_status=${PIPESTATUS[0]}
        if [ "${archive_status}" -ne 0 ]; then
            echo "ERROR: Search archive generation failed (exit ${archive_status})" >&2
            rm -rf "${data_dir}"
            exit "${archive_status}"
        fi

        # Verify archive was created
        if [ ! -f "${archive_output}" ]; then
            echo "ERROR: Search archive file not found: ${archive_output}" >&2
            echo "Current directory contents:" >&2
            ls -lh >&2
            rm -rf "${data_dir}"
            exit 1
        fi

        # Verify library was created
        if [ ! -f "${library_output}" ]; then
            echo "ERROR: Library file not found: ${library_output}" >&2
            echo "Current directory contents:" >&2
            ls -lh >&2
            rm -rf "${data_dir}"
            exit 1
        fi

        # # If above code fails to locate the generated archive and spectral library, use "find" to look deeper into subdirectories 
        # if [ ! -f "${archive_output}" ]; then
        #     echo "Archive not in current directory, searching..." >&2
        #     found_archive=$(find . -type f -name "*.psar" | head -n 1)
        #     if [ -z "${found_archive}" ]; then
        #         echo "ERROR: Search archive file not found: ${archive_output}" >&2
        #         echo "Current directory contents:" >&2
        #         ls -lhR >&2
        #         rm -rf "${data_dir}"
        #         exit 1
        #     fi
        #     cp "${found_archive}" "${archive_output}"
        # fi

        echo "Search archive generated: ${archive_output}" >&2
        ls -lh "${archive_output}" >&2
        echo "Library generated: ${library_output}" >&2
        ls -lh "${library_output}" >&2

        # Cleanup
        rm -rf "${data_dir}"
    >>>

    output {
        File search_archives = sub(basename(input_file), "\\.(htrms|raw|d|HTRMS|RAW|D)$", ""
            ) + ".psar"
        # File spectral_libraries = sub(basename(input_file), "\\.(htrms|raw|d|HTRMS|RAW|D)$", ""
        #     ) + ".kit"
    }

    runtime {
        docker: "cameronlian/panoply-spectronaut:v20.0"
        cpu: 128 # archive generation is CPU intensive 
        memory: "256GB"
        bootDiskSizeGb: 128
        disks: "local-disk ~{disk_gb} SSD"
        preemptible: 0
    }
}

task merge_archives {
    input {
        Array[File] input_archives
    }

    command <<<
                set -euo pipefail

                echo "=== Merging search archives ===" >&2
                echo "Number of archives: ~{length(input_archives)}" >&2

                if [ ~{length(input_archives)} -eq 0 ]; then
                    echo "ERROR: No search archives provided for merging" >&2
                    exit 1
                fi

                output_dir=$(mktemp -d output_XXXXXX)
                merged_library="merged_library.kit"

                # Build command with multiple -sa flags
                python3 - <<'PY'
        import json
        from pathlib import Path

        archives = json.loads(r'''~{write_json(input_archives)}''')
        Path("archives_list.txt").write_text(
            "\n".join(archives) + ("\n" if archives else ""),
            encoding="utf-8",
        )
        PY

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
                done < archives_list.txt

                # Build -sa arguments
                sa_args=""
                while IFS= read -r archive; do
                    [ -z "${archive}" ] && continue
                    sa_args="${sa_args} -sa \"${archive}\""
                done < archives_list.txt

                echo "Merging archives into ${merged_library}..." >&2
                # shellcheck disable=SC2086
                eval spectronaut lg -se Pulsar \
                    ${sa_args} \
                    -k "${merged_library}" \
                    -o "${output_dir}" 2>&1 | tee merge_archives.log

                merge_status=${PIPESTATUS[0]}
                if [ "${merge_status}" -ne 0 ]; then
                    echo "ERROR: Archive merging failed (exit ${merge_status})" >&2
                    rm -rf "${output_dir}"
                    exit "${merge_status}"
                fi

                # Verify merged library was created
                if [ ! -f "${merged_library}" ]; then
                    echo "ERROR: Merged library file not found: ${merged_library}" >&2
                    echo "Output directory contents:" >&2
                    find "${output_dir}" -type f >&2
                    rm -rf "${output_dir}"
                    exit 1
                fi

                echo "Merged library created successfully:" >&2
                ls -lh "${merged_library}" >&2

                # Cleanup
                rm -rf "${output_dir}"
    >>>

    output {
        File merged_archive = "merged_library.kit"
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
        File search_archive
        File input_file # Cromwell automatically localizes File inputs to the task execution directory
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

        echo "=== Spectronaut DIA analysis ===" >&2
        echo "Input file: ~{input_file}" >&2
        echo "Search archive: ~{search_archive}" >&2

        data_dir=$(mktemp -d data_XXXXXX)
        output_dir=$(mktemp -d output_XXXXXX)
        temp_dir=$(mktemp -d temp_XXXXXX)

        # Copy input file to data directory (handles both regular files and timsTOF .d directories)
        echo "Copying input file to data directory..." >&2
        if ! cp -r "~{input_file}" "${data_dir}/"; then
            echo "ERROR: Failed to copy input file" >&2
            rm -rf "${data_dir}" "${output_dir}" "${temp_dir}"
            exit 1
        fi

        echo "Data directory contents:" >&2
        ls -lh "${data_dir}" >&2

        # Generate unique experiment name to avoid .sne file collisions
        input_basename=$(basename "~{input_file}")
        file_basename="${input_basename%.*}"  # Remove extension
        unique_exp_name="~{experiment_name}_${file_basename}"

        echo "Unique experiment name: ${unique_exp_name}" >&2

        echo "Running DIA analysis..." >&2
        spectronaut diaanalysis \
            -o "${output_dir}" \
            -a "~{search_archive}" \
            -d "${data_dir}" \
            ~{if defined(analysis_schema) then "-s " + analysis_schema else ""} \
            -fasta "~{fasta_1}" \
            ~{if defined(fasta_2) then "-fasta " + fasta_2 else ""} \
            ~{if defined(fasta_3) then "-fasta " + fasta_3 else ""} \
            ~{if defined(json_settings) then "-j " + json_settings else ""} \
            -n "${unique_exp_name}" \
            -setTemp "${temp_dir}" 2>&1 | tee dia_analysis.log

        dia_status=${PIPESTATUS[0]}
        if [ "${dia_status}" -ne 0 ]; then
            echo "ERROR: DIA analysis failed (exit ${dia_status})" >&2
            rm -rf "${data_dir}" "${output_dir}" "${temp_dir}"
            exit "${dia_status}"
        fi

        # Find the .sne file (may be in timestamped subdirectory)
        echo "Locating SNE file in output directory..." >&2
        sne_file=$(find "${output_dir}" -type f -name "*.sne" | head -n 1) # Look deep into subdirectories 

        if [ -z "${sne_file}" ]; then
            echo "ERROR: No SNE file produced" >&2
            echo "Output directory contents:" >&2
            find "${output_dir}" -type f >&2
            rm -rf "${data_dir}" "${output_dir}" "${temp_dir}"
            exit 1
        fi

        # Copy SNE file to expected location
        output_sne="${unique_exp_name}.sne"
        echo "Copying SNE file: ${sne_file} -> ${output_sne}" >&2
        if ! cp "${sne_file}" "${output_sne}"; then
            echo "ERROR: Failed to copy SNE file" >&2
            rm -rf "${data_dir}" "${output_dir}" "${temp_dir}"
            exit 1
        fi

        echo "DIA analysis complete. SNE file:" >&2
        ls -lh "${output_sne}" >&2

        # Cleanup
        rm -rf "${data_dir}" "${output_dir}" "${temp_dir}"
    >>>

    output {
        File sne_files = experiment_name + "_" + sub(basename(input_file), "\\.(htrms|raw|d|HTRMS|RAW|D)$", "") + ".sne"
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

task sne_combine {
    input {
        File fasta_1
        Array[File] input_sne
        String experiment_name
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

                echo "=== Combining SNE files ===" >&2
                echo "Experiment name: ~{experiment_name}" >&2
                echo "SNE inputs: ~{length(input_sne)}" >&2

                if [ ~{length(input_sne)} -eq 0 ]; then
                    echo "ERROR: No SNE files provided for combination" >&2
                    exit 1
                fi

                out_dir="combined_output"
                out_zip="spectronaut_output.zip"

                mkdir -p "${out_dir}"

                # Create list of SNE files
                python3 - <<'PY'
        import json
        from pathlib import Path

        sne_files = json.loads(r'''~{write_json(input_sne)}''')
        Path("sne_inputs.txt").write_text(
            "\n".join(sne_files) + ("\n" if sne_files else ""),
            encoding="utf-8",
        )
        PY

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

                echo "Running Spectronaut combine..." >&2
                # shellcheck disable=SC2086
                eval spectronaut combine \
                    -n "~{experiment_name}" \
                    -o "${out_dir}" \
                    ${sne_args} \
                    -fasta "~{fasta_1}" \
                    ~{if defined(fasta_2) then "-fasta " + fasta_2 else ""} \
                    ~{if defined(fasta_3) then "-fasta " + fasta_3 else ""} \
                    ~{if defined(analysis_schema) then "-s " + analysis_schema else ""} \
                    ~{if defined(report_schema_1) then "-rs " + report_schema_1 else ""} \
                    ~{if defined(report_schema_2) then "-rs " + report_schema_2 else ""} \
                    ~{if defined(report_schema_3) then "-rs " + report_schema_3 else ""} \
                    ~{if defined(report_schema_4) then "-rs " + report_schema_4 else ""} 2>&1 | tee spectronaut_combine.log

                combine_status=${PIPESTATUS[0]}
                echo "Spectronaut combine status: ${combine_status}" >&2

                if [ "${combine_status}" -ne 0 ]; then
                    echo "ERROR: Spectronaut combine failed" >&2
                    exit "${combine_status}"
                fi

                if [ -z "$(find "${out_dir}" -type f -name '*.sne')" ]; then
                    echo "ERROR: Combine step did not produce any SNE files" >&2
                    exit 1
                fi

                echo "Combined output contents:" >&2
                ls -lhR "${out_dir}" >&2

                # Compress output using pattern from Spectronaut_v20-0.wdl
                echo "Compressing output to ${out_zip}..." >&2
                if ! zip -r "${out_zip}" "${out_dir}" -x \*.zip; then
                    echo "ERROR: Failed to create archive ${out_zip}" >&2
                    exit 1
                fi

                if [ ! -f "${out_zip}" ]; then
                    echo "ERROR: Archive not found after creation: ${out_zip}" >&2
                    exit 1
                fi

                echo "Archive created successfully:" >&2
                ls -lh "${out_zip}" >&2
    >>>

    output {
        File spectronaut_output = "spectronaut_output.zip"
    }

    runtime {
        docker: "cameronlian/panoply-spectronaut:v20.0"
        cpu: 32
        memory: "512GB"
        bootDiskSizeGb: 512
        disks: "local-disk ~{disk_gb} SSD"
        preemptible: 0
    }
}

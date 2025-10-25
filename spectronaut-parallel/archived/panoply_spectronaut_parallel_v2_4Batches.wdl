version development

workflow panoply_spectronaut_parallel {
    input {
        # Control flags
        Boolean do_conversion = true
        Boolean do_search = true

        # Input file selection
        String? files_directory             # GCS path to directory containing all input files (e.g., gs://bucket/raw-data/)

        # Experiment configuration
        String experiment_name

        # HTRMS conversion inputs
        File? convert_schema

        # Spectronaut search inputs
        File? analysis_settings
        File? condition_setup

        # FASTA databases (fasta_1 is required, others optional)
        File fasta_1
        File? fasta_2
        File? fasta_3

        # Enzyme database
        File? enzyme_database

        # Spectral libraries (if not provided, runs DirectDIA)
        File? spectral_library_1
        File? spectral_library_2

        # Report schemas
        File? report_schema_1
        File? report_schema_2

        # JSON settings
        File? json_settings

        # VM configuration
        Int n_vms = 4                            # Number of VMs to use for parallel search (default: 4)

        # Resource configurations for search (per file)
        Int search_cpu_per_vm = 32
        Int search_memory_gb_per_vm = 256

        # Dynamic disk sizing
        Int file_size_multiplier = 3
    }

    # List files from GCS directory
    if (defined(files_directory)) {
        call list_files {
            input:
                gcs_path = select_first([files_directory])
        }
    }

    # Use discovered files (workflow will fail if files_directory is not provided)
    Array[String] input_files = select_first([list_files.input_files])

    # Step 1: Convert to HTRMS if requested (scatter over each file)
    if (do_conversion) {
        scatter (file in input_files) {
            call convert_htrms_single {
                input:
                    raw_file_gcs_path = file,
                    convert_schema = convert_schema,
                    disk_gb = 200
            }
        }
    }

    # Determine which files to use for search (HTRMS if converted, otherwise raw)
    Array[String] files_to_search = select_first([convert_htrms_single.htrms_gcs_path, input_files])

    # Calculate disk per batch VM
    # Note: With String GCS paths, we can't use size() function
    # Using conservative fixed disk size instead
    Int search_disk_gb_per_batch = 2000  # 2TB fixed disk per batch VM

    # Step 3: Run Spectronaut search in parallel across VMs
    # Two scenarios: n_vms > 1 (batching) and n_vms = 1 (single VM)
    if (do_search) {
        # For n_vms > 1: Create batch lists and scatter across VMs
        if (n_vms > 1) {
            call make_batches {
                input:
                    files = files_to_search,
                    n_batches = n_vms
            }

            # Launch one VM per batch; each VM processes its list sequentially
            scatter (batch_list in make_batches.batch_lists) {
                call spectronaut_search_batch {
                    input:
                        list_file = batch_list,
                        all_files = files_to_search,
                        experiment_name = experiment_name,
                        analysis_settings = analysis_settings,
                        fasta_1 = fasta_1,
                        fasta_2 = fasta_2,
                        fasta_3 = fasta_3,
                        enzyme_database = enzyme_database,
                        spectral_library_1 = spectral_library_1,
                        spectral_library_2 = spectral_library_2,
                        report_schema_1 = report_schema_1,
                        report_schema_2 = report_schema_2,
                        json_settings = json_settings,
                        cpu = search_cpu_per_vm,
                        memory_gb = search_memory_gb_per_vm,
                        disk_gb = search_disk_gb_per_batch
                }
            }
        }

        # For n_vms = 1: Process all files in a single VM (no batching needed)
        if (n_vms == 1) {
            call create_single_batch {
                input:
                    files = files_to_search
            }

            call spectronaut_search_batch as spectronaut_search_single {
                input:
                    list_file = create_single_batch.batch_list,
                    all_files = files_to_search,
                    experiment_name = experiment_name,
                    analysis_settings = analysis_settings,
                    fasta_1 = fasta_1,
                    fasta_2 = fasta_2,
                    fasta_3 = fasta_3,
                    enzyme_database = enzyme_database,
                    spectral_library_1 = spectral_library_1,
                    spectral_library_2 = spectral_library_2,
                    report_schema_1 = report_schema_1,
                    report_schema_2 = report_schema_2,
                    json_settings = json_settings,
                    cpu = search_cpu_per_vm,
                    memory_gb = search_memory_gb_per_vm,
                    disk_gb = search_disk_gb_per_batch
            }
        }

        # Collect SNE outputs from either path
        Array[Array[File]]? multi_vm_sne_outputs = if (n_vms > 1) then spectronaut_search_batch.sne_outputs else None
        Array[File]? single_vm_sne_outputs = if (n_vms == 1) then spectronaut_search_single.sne_outputs else None

        Array[File] all_sne_files = select_first([
            if defined(multi_vm_sne_outputs) then flatten(select_first([multi_vm_sne_outputs])) else None,
            single_vm_sne_outputs
        ])

        # Step 4: Combine all SNE files (reports generated during combine via -rs flags)
        call combine_sne_files {
            input:
                sne_files = all_sne_files,
                experiment_name = experiment_name,
                analysis_settings = analysis_settings,
                fasta_1 = fasta_1,
                fasta_2 = fasta_2,
                fasta_3 = fasta_3,
                report_schema_1 = report_schema_1,
                report_schema_2 = report_schema_2
        }

        # Step 5: Zip final output
        call zip_final_output {
            input:
                combined_dir = combine_sne_files.combined_output,
                experiment_name = experiment_name
        }
    }

    output {
        File? spectronaut_output = zip_final_output.output_zip
    }

    meta {
        author: "C. Lian"
        email: "proteogenomics@broadinstitute.org"
    }
}

# Task 1: List all files in GCS directory (fast, no data transfer)
task list_files {
    input {
        String gcs_path     # GCS path like gs://bucket/raw-data/
    }

    command <<<
        set -euo pipefail

        echo "=== Discovering files in GCS (no localization) ===" >&2
        echo "GCS path: ~{gcs_path}" >&2

        # Ensure path ends with / for proper gcloud storage ls behavior
        GCS_PATH="~{gcs_path}"
        if [[ ! "$GCS_PATH" =~ /$ ]]; then
            GCS_PATH="${GCS_PATH}/"
        fi

        echo "Normalized path: ${GCS_PATH}" >&2

        # Use gcloud storage ls to list files directly from GCS (API calls only, no data transfer)
        # Find .raw files (Thermo Astral format - single file)
        gcloud storage ls "${GCS_PATH}*.raw" 2>/dev/null > raw_files.txt || true
        gcloud storage ls "${GCS_PATH}*.RAW" 2>/dev/null >> raw_files.txt || true

        # Find .d directories (Bruker timsTOF format - directory with .d extension)
        # Note: gcloud storage ls shows directories with trailing / - we need to remove it for WDL File type
        gcloud storage ls "${GCS_PATH}*.d/" 2>/dev/null > d_folders_raw.txt || true

        # Strip trailing slashes from .d directory paths for WDL File type compatibility
        # Cromwell's File type regex requires paths to NOT end with /
        sed 's:/$::' d_folders_raw.txt > d_folders.txt

        # Combine both lists
        cat raw_files.txt d_folders.txt > file_list.txt

        # Remove empty lines and duplicates
        sed -i '/^$/d' file_list.txt
        sort -u file_list.txt -o file_list.txt

        # Count the files/folders
        file_count=$(wc -l < file_list.txt | tr -d ' ')
        echo "Found ${file_count} files/folders to process" >&2

        # Validate that files were found
        if [ "${file_count}" -eq 0 ]; then
            echo "ERROR: No .raw files or .d/ directories found in: ${GCS_PATH}" >&2
            echo "Attempting to list bucket contents for debugging:" >&2
            gcloud storage ls "${GCS_PATH}" 2>&1 | head -20 >&2
            exit 1
        fi

        # Show first few files for verification
        echo "Files/folders found:" >&2
        echo "  .raw files: $(grep -c '\.raw$\|\.RAW$' file_list.txt || echo 0)" >&2
        echo "  .d folders: $(grep -c '\.d/$' file_list.txt || echo 0)" >&2
        echo "" >&2
        echo "First 10 GCS paths:" >&2
        head -10 file_list.txt >&2

        # Output each GCS path on a separate line
        # These paths will be localized by Cromwell when needed (efficient)
        cat file_list.txt
    >>>

    output {
        Array[String] input_files = read_lines(stdout())
    }

    runtime {
        docker: "google/cloud-sdk:slim"
        cpu: 2
        memory: "4GB"
        disks: "local-disk 20 SSD"
        preemptible: 0
    }

    meta {
        author: "C. Lian"
        email: "proteogenomics@broadinstitute.org"
    }
}

# Task 1b: Create a single batch list with all files (for n_vms=1)
task create_single_batch {
    input {
        Array[String] files    # GCS paths
    }

    command <<<
        set -euo pipefail

        echo "=== Creating single batch with all files ===" >&2

        # Extract basenames from file paths
        cat > all_files_full.txt <<'EOF'
~{sep('\n', files)}
EOF

        # Extract just the basenames
        while IFS= read -r filepath; do
          basename "$filepath"
        done < all_files_full.txt > batch_list.txt

        # Count files
        file_count=$(wc -l < batch_list.txt | tr -d ' ')
        echo "Total files in single batch: ${file_count}" >&2

        # Show files
        echo "Files to process:" >&2
        cat batch_list.txt >&2
    >>>

    output {
        File batch_list = "batch_list.txt"
    }

    runtime {
        docker: "ubuntu:22.04"
        cpu: 2
        memory: "8GB"
        disks: "local-disk 200 SSD"
        preemptible: 0
    }

    meta {
        author: "C. Lian"
        email: "proteogenomics@broadinstitute.org"
    }
}

# Task 2: Convert single file to HTRMS format (manual GCS localization)
task convert_htrms_single {
    input {
        String raw_file_gcs_path    # GCS path (gs://...)
        File? convert_schema
        Int disk_gb
    }

    command <<<
        set -euo pipefail

        echo "=== Converting to HTRMS (broadcptacdev/panoply_spectronaut:v20.0) ===" >&2
        echo "GCS path: ~{raw_file_gcs_path}" >&2

        # Get basename and determine expected output filename
        input_basename=$(basename ~{raw_file_gcs_path})
        expected_output=$(echo "${input_basename}" | sed -E 's/\.(raw|d|RAW|D)$/.htrms/')

        echo "Input file: ${input_basename}" >&2
        echo "Expected output: ${expected_output}" >&2
        echo "Execution directory: $(pwd)" >&2
        echo "Available disk space:" >&2
        df -h >&2

        # Create temporary working directories
        work_dir=$(mktemp -d work_XXXXXX)
        input_dir="${work_dir}/input"
        output_dir="${work_dir}/output"
        mkdir -p "${input_dir}" "${output_dir}"

        echo "Working directory: ${work_dir}" >&2

        # Download file from GCS into temporary input directory
        echo "Downloading from GCS..." >&2
        if ! gcloud storage cp -r ~{raw_file_gcs_path} "${input_dir}/"; then
            echo "ERROR: Failed to download from GCS: ~{raw_file_gcs_path}" >&2
            rm -rf "${work_dir}"
            exit 1
        fi

        echo "Downloaded successfully" >&2
        echo "Input directory contents:" >&2
        ls -lhR "${input_dir}/" >&2

        # Run Spectronaut conversion
        # Spectronaut will look inside input_dir and find the .raw or .d file
        echo "Running spectronaut -convert..." >&2
        spectronaut -convert \
            -i "${input_dir}" \
            -o "${output_dir}" \
            ~{if defined(convert_schema) then "-s " + convert_schema else ""} 2>&1 | tee spectronaut_convert.log >&2

        conversion_status=${PIPESTATUS[0]}
        echo "Spectronaut conversion exit status: ${conversion_status}" >&2

        if [ "${conversion_status}" -ne 0 ]; then
            echo "ERROR: Spectronaut conversion failed with exit code ${conversion_status}" >&2
            cat spectronaut_convert.log >&2
            rm -rf "${work_dir}"
            exit "${conversion_status}"
        fi

        echo "=== Conversion complete ===" >&2
        echo "Output directory contents:" >&2
        ls -lhR "${output_dir}/" >&2

        # Find the converted .htrms file
        converted_file=$(find "${output_dir}" -type f -name "*.htrms" | head -n 1)

        if [ -z "$converted_file" ]; then
            echo "ERROR: No .htrms file found in output directory" >&2
            find "${output_dir}" -type f >&2
            rm -rf "${work_dir}"
            exit 1
        fi

        echo "Found converted file: ${converted_file}" >&2

        # Copy the converted file to execution directory with correct name
        echo "Copying output file to execution directory..." >&2
        if ! cp "${converted_file}" "${expected_output}"; then
            echo "ERROR: Failed to copy output file" >&2
            rm -rf "${work_dir}"
            exit 1
        fi

        # Cleanup temporary working directory
        rm -rf "${work_dir}"

        # Verify output file exists with correct name
        if [ ! -f "${expected_output}" ]; then
            echo "ERROR: Output file not found: ${expected_output}" >&2
            echo "Contents of execution directory:" >&2
            ls -lh . >&2
            exit 1
        fi

        echo "=== Success ===" >&2
        echo "Output file: ${expected_output}" >&2
        ls -lh "${expected_output}" >&2
    >>>

    output {
        # Handle both .raw files and .d directories - strip extension and add .htrms
        File htrms_file = sub(basename(raw_file_gcs_path), "\\.(raw|d|RAW|D)$", "") + ".htrms"
        # Return GCS path for downstream tasks (Cromwell will upload htrms_file and we construct the expected path)
        String htrms_gcs_path = sub(raw_file_gcs_path, "\\.(raw|d|RAW|D)$", "") + ".htrms"
    }

    runtime {
        docker: "broadcptacdev/panoply_spectronaut:v20.0"
        cpuPlatform: "AMD Rome"
        cpu: 8
        memory: "32GB"
        bootDiskSizeGb: 512
        disks: "local-disk ~{disk_gb} SSD"
        preemptible: 0
    }

    meta {
        author: "C. Lian"
        email: "proteogenomics@broadinstitute.org"
    }
}

# Task: Create N batch lists from a list of files
task make_batches {
    input {
        Array[String] files    # GCS paths
        Int n_batches = 4
    }

    command <<<
        set -euo pipefail

        echo "=== Creating ~{n_batches} batches ===" >&2
        mkdir -p batches

        # Extract basenames from file paths (not full paths - those won't exist in downstream VMs)
        # Write basenames to all_files.txt for batch distribution
        cat > all_files_full.txt <<'EOF'
~{sep('\n', files)}
EOF

        # Extract just the basenames
        while IFS= read -r filepath; do
          basename "$filepath"
        done < all_files_full.txt > all_files.txt

        # Count lines (total number of files)
        total=$(wc -l < all_files.txt | tr -d ' ')
        echo "Total files: ${total}" >&2

        if [ "${total}" -eq 0 ]; then
          echo "No files to batch; leaving empty batch lists" >&2
          cp all_files.txt batches/batch_00.txt
        else
          # Use AWK for efficient single-pass distribution of basenames
          awk -v n_batches=~{n_batches} 'BEGIN {
            for (i = 0; i < n_batches; i++) {
              printf "" > sprintf("batches/batch_%02d.txt", i)
            }
          }
          {
            batch = (NR - 1) % n_batches
            print > sprintf("batches/batch_%02d.txt", batch)
          }' all_files.txt
        fi

        echo "Batch distribution:" >&2
        for bl in batches/batch_*.txt; do
          echo "  $(basename "$bl"): $(wc -l < "$bl") files" >&2
        done
    >>>

    output {
        Array[File] batch_lists = glob("batches/batch_*.txt")
    }

    runtime {
        docker: "ubuntu:22.04"
        cpu: 2
        memory: "8GB"
        disks: "local-disk 200 SSD"
        preemptible: 0
    }

    meta {
        author: "C. Lian"
        email: "proteogenomics@broadinstitute.org"
    }
}

# Task: Run Spectronaut search sequentially over a batch list
task spectronaut_search_batch {
    input {
        File list_file              # Text file with basenames to process in this batch
        Array[String] all_files     # All GCS file paths (manually downloaded)
        String experiment_name

        File? analysis_settings
        File fasta_1
        File? fasta_2
        File? fasta_3
        File? enzyme_database
        File? spectral_library_1
        File? spectral_library_2
        File? report_schema_1
        File? report_schema_2
        File? json_settings

        Int cpu
        Int memory_gb
        Int disk_gb
    }

    command <<<
        set -euo pipefail

        echo "=== Spectronaut batch search (broadcptacdev/panoply_spectronaut:v20.0) ===" >&2
        echo "Batch list file: ~{list_file}" >&2

        if [ ! -f ~{list_file} ]; then
          echo "ERROR: list_file not found: ~{list_file}" >&2
          exit 1
        fi

        # Create GCS files map (basename -> GCS path)
        cat > all_gcs_files.txt <<'EOF'
~{sep('\n', all_files)}
EOF

        echo "All GCS file paths available:" >&2
        cat all_gcs_files.txt >&2
        echo "" >&2

        # Read basenames for this batch
        mapfile -t basenames < ~{list_file}
        echo "Basenames in this batch: ${#basenames[@]}" >&2
        cat ~{list_file} >&2
        echo "" >&2

        mkdir -p outputs

        if [ ${#basenames[@]} -eq 0 ]; then
          echo "Empty batch; nothing to do." >&2
          exit 0
        fi

        cromwell_root=$(pwd)
        echo "Cromwell root: ${cromwell_root}" >&2

        # Optional: import enzyme DB once per batch
        ~{if defined(enzyme_database) then "dotnet /usr/lib/spectronaut/SpectronautCMD.dll --importEnzymeDB " + enzyme_database else ""}

        # Process each file in this batch
        for target_basename in "${basenames[@]}"; do
          # Find the GCS path matching this basename
          gcs_path=""
          while IFS= read -r gcs_file; do
            if [ "$(basename "$gcs_file")" == "$target_basename" ]; then
              gcs_path="$gcs_file"
              break
            fi
          done < all_gcs_files.txt

          # Validate GCS path was found
          if [ -z "$gcs_path" ]; then
            echo "ERROR: No GCS path found for basename: $target_basename" >&2
            exit 1
          fi

          input_basename=$(basename "$gcs_path")
          # Strip extension to avoid double extensions like .htrms.sne
          base_name="${input_basename%.*}"
          file_exp_name="~{experiment_name}_${base_name}"

          echo "--- Processing ${input_basename} ---" >&2
          echo "Matched basename '${target_basename}' to GCS path: ${gcs_path}" >&2
          echo "Available disk space:" >&2
          df -h >&2

          # Fresh working dirs per file to limit disk usage
          sn_temp=$(mktemp -d sn_temp_XXXXXX)
          work=$(mktemp -d work_XXXXXX)
          mkdir -p "${work}/data" "${work}/spectronaut_out"

          # Download from GCS using gcloud storage cp (handles both files and .d directories)
          echo "Downloading from GCS: ${gcs_path}" >&2
          if ! gcloud storage cp -r "${gcs_path}" "${work}/data/"; then
            echo "ERROR: Failed to download from GCS: ${gcs_path}" >&2
            rm -rf "${work}" "${sn_temp}"
            exit 1
          fi

          echo "Successfully downloaded from GCS" >&2
          echo "Data directory contents:" >&2
          ls -lhR "${work}/data" >&2

          # Run Spectronaut
          spectronaut \
            ~{if !defined(spectral_library_1) then "-direct" else ""} \
            ~{if defined(analysis_settings) then "-s " + analysis_settings else ""} \
            ~{if defined(fasta_2) then "-fasta " + fasta_2 else ""} \
            ~{if defined(fasta_3) then "-fasta " + fasta_3 else ""} \
            ~{if defined(spectral_library_1) then "-a " + spectral_library_1 else ""} \
            ~{if defined(spectral_library_2) then "-a " + spectral_library_2 else ""} \
            ~{if defined(report_schema_1) then "-rs " + report_schema_1 else ""} \
            ~{if defined(report_schema_2) then "-rs " + report_schema_2 else ""} \
            ~{if defined(json_settings) then "-j " + json_settings else ""} \
            -n "${file_exp_name}" \
            -o "${work}/spectronaut_out" \
            -fasta ~{fasta_1} \
            -d "${work}/data" \
            -setTemp "${sn_temp}" 2>&1 | tee -a spectronaut_batch.log >&2

          status=${PIPESTATUS[0]}
          echo "Spectronaut status for ${input_basename}: ${status}" >&2

          if [ "$status" -ne 0 ]; then
            echo "ERROR: Spectronaut failed for ${input_basename} (exit ${status})" >&2
            rm -rf "${work}" "${sn_temp}"
            exit "$status"
          fi

          # Move SNE to outputs with stable name
          echo "Searching for SNE file in ${work}/spectronaut_out..." >&2
          sne_file=$(find "${work}/spectronaut_out" -type f -name "*.sne" | head -n 1)
          if [ -z "$sne_file" ]; then
            echo "ERROR: No .sne found for ${input_basename}" >&2
            echo "Contents of spectronaut_out:" >&2
            find "${work}/spectronaut_out" -type f >&2
            rm -rf "${work}" "${sn_temp}"
            exit 1
          fi

          out_name="${file_exp_name}.sne"
          echo "Moving ${sne_file} to outputs/${out_name}..." >&2
          mv "$sne_file" "outputs/${out_name}"

          # Verify the file was moved successfully
          if [ ! -f "outputs/${out_name}" ]; then
            echo "ERROR: Failed to move SNE file to outputs directory" >&2
            exit 1
          fi

          echo "✓ Saved: outputs/${out_name} ($(du -h "outputs/${out_name}" | cut -f1))" >&2

          # Cleanup per-file working space to keep disk bounded
          rm -rf "${work}" "${sn_temp}"
        done

        echo "" >&2
        echo "=== Batch processing complete ===" >&2

        # Verify outputs directory exists and has files
        if [ ! -d "outputs" ]; then
          echo "WARNING: outputs directory does not exist, creating it..." >&2
          mkdir -p outputs
        fi

        sne_count=$(find outputs -name "*.sne" -type f | wc -l)
        echo "Total SNE files created: ${sne_count}" >&2

        if [ "${sne_count}" -eq 0 ]; then
          echo "WARNING: No SNE files were created in this batch" >&2
          echo "This could indicate all files in the batch failed to process" >&2
        else
          echo "" >&2
          echo "SNE files in outputs directory:" >&2
          ls -lh outputs/ >&2
          echo "" >&2
          echo "Verifying all SNE files are readable:" >&2
          for sne in outputs/*.sne; do
            if [ -f "$sne" ]; then
              echo "  ✓ $(basename "$sne"): $(du -h "$sne" | cut -f1)" >&2
            fi
          done
        fi
    >>>

    output {
        Array[File] sne_outputs = glob("outputs/*.sne")
    }

    runtime {
        docker: "broadcptacdev/panoply_spectronaut:v20.0"
        cpuPlatform: "AMD Rome"
        cpu: cpu
        memory: "~{memory_gb}GB"
        bootDiskSizeGb: 512
        disks: "local-disk ~{disk_gb} SSD"
        preemptible: 0
    }

    parameter_meta {
        all_files: {
            stream: true
        }
    }

    meta {
        author: "C. Lian"
        email: "proteogenomics@broadinstitute.org"
    }
}

# Task 3: Combine all SNE files into one
task combine_sne_files {
    input {
        Array[File] sne_files
        String experiment_name
        File? analysis_settings

        File fasta_1
        File? fasta_2
        File? fasta_3
        File? report_schema_1
        File? report_schema_2
    }

    command <<<
        set -euo pipefail

        echo "=== Combining SNE files (broadcptacdev/panoply_spectronaut:v20.0) ===" >&2
        echo "Number of SNE files: ~{length(sne_files)}" >&2
        echo "Experiment name: ~{experiment_name}" >&2

        # Check if any SNE files were provided
        if [ ~{length(sne_files)} -eq 0 ]; then
            echo "ERROR: No SNE files provided to combine task" >&2
            # echo "This usually means:" >&2
            # echo "  1. All search batches failed to produce SNE files" >&2
            # echo "  2. The search tasks did not complete successfully" >&2
            # echo "  3. The batch processing had no input files" >&2
            # exit 1
        fi

        # List all SNE files before validation (for debugging)
        echo "SNE files to combine (localized paths):" >&2
        cat > sne_list.txt <<'EOF'
~{sep('\n', sne_files)}
EOF
        cat sne_list.txt >&2

        # Validate input SNE files exist and are accessible
        echo "" >&2
        echo "Validating input SNE files..." >&2
        while IFS= read -r sne_file; do
            if [ ! -f "$sne_file" ]; then
                echo "ERROR: SNE file not found: $sne_file" >&2
                echo "Current working directory: $(pwd)" >&2
                echo "Files in working directory:" >&2
                ls -lhR . >&2
                exit 1
            fi
            echo "  ✓ $(basename "$sne_file"): $(du -h "$sne_file" | cut -f1)" >&2
        done < sne_list.txt

        out_dir="combined_output"
        mkdir -p ${out_dir}

        echo "" >&2
        echo "Available disk space:" >&2
        df -h >&2
        echo "Available memory:" >&2
        free -h >&2

        # Build SNE file arguments from validated list
        echo "" >&2
        echo "Building spectronaut command..." >&2
        sne_args=""
        while IFS= read -r sne_file; do
            sne_args="${sne_args} -sne \"${sne_file}\""
        done < sne_list.txt

        echo "SNE arguments: ${sne_args}" >&2

        # Execute combine command with output capture
        echo "" >&2
        echo "Running SNE combination..." >&2
        # shellcheck disable=SC2086
        eval spectronaut -combine \
            -n ~{experiment_name} \
            -o ${out_dir} \
            ~{if defined(analysis_settings) then "-s " + analysis_settings else ""} \
            ${sne_args} \
            -fasta ~{fasta_1} \
            ~{if defined(fasta_2) then "-fasta " + fasta_2 else ""} \
            ~{if defined(fasta_3) then "-fasta " + fasta_3 else ""} \
            ~{if defined(report_schema_1) then "-rs " + report_schema_1 else ""} \
            ~{if defined(report_schema_2) then "-rs " + report_schema_2 else ""} 2>&1 | tee spectronaut_combine.log >&2

        combine_status=${PIPESTATUS[0]}
        echo "Spectronaut combine exit status: ${combine_status}" >&2

        if [ "${combine_status}" -ne 0 ]; then
            echo "ERROR: Spectronaut combine failed with exit code ${combine_status}" >&2
            echo "=== Spectronaut output log ===" >&2
            cat spectronaut_combine.log >&2
            echo "=== Disk space after failure ===" >&2
            df -h >&2
            exit "${combine_status}"
        fi

        echo "=== SNE combination complete ===" >&2

        # Validate output was created
        if [ ! -d "${out_dir}" ] || [ -z "$(ls -A ${out_dir})" ]; then
            echo "ERROR: Output directory is empty or does not exist" >&2
            exit 1
        fi

        # List output contents
        echo "Output directory contents:" >&2
        ls -lhR ${out_dir}/ >&2
    >>>

    output {
        Directory combined_output = "combined_output"
    }

    runtime {
        docker: "broadcptacdev/panoply_spectronaut:v20.0"
        cpuPlatform: "AMD Rome"
        cpu: 32
        memory: "256GB"
        bootDiskSizeGb: 512
        disks: "local-disk 2000 SSD"
        preemptible: 0
    }

    meta {
        author: "C. Lian"
        email: "proteogenomics@broadinstitute.org"
    }
}

# Task 4: Zip final output (combined SNE and reports)
task zip_final_output {
    input {
        Directory combined_dir
        String experiment_name
    }

    command <<<
        set -euo pipefail

        echo "=== Creating final output zip ===" >&2
        echo "Experiment name: ~{experiment_name}" >&2

        # Validate input directory
        if [ ! -d ~{combined_dir} ]; then
            echo "ERROR: Combined directory does not exist: ~{combined_dir}" >&2
            exit 1
        fi

        if [ -z "$(ls -A ~{combined_dir})" ]; then
            echo "ERROR: Combined directory is empty: ~{combined_dir}" >&2
            exit 1
        fi

        echo "Combined directory contents:" >&2
        ls -lhR ~{combined_dir}/ >&2

        echo "Available disk space:" >&2
        df -h >&2

        final_output_dir="~{experiment_name}_output"
        mkdir -p ${final_output_dir}

        # Copy combined SNE results (includes reports generated during combine)
        echo "Copying combined output (SNE + reports)..." >&2
        if ! cp -r ~{combined_dir}/* ${final_output_dir}/; then
            echo "ERROR: Failed to copy combined output" >&2
            exit 1
        fi

        echo "Files to be zipped:" >&2
        ls -lhR ${final_output_dir}/ >&2

        # Create archive with parallel compression if available, excluding any nested zip files
        output_zip="spectronaut_output.zip"
        echo "Creating archive..." >&2

        # Use pigz for parallel compression if available, otherwise standard zip
        if command -v pigz &> /dev/null; then
            echo "Using parallel compression (tar + pigz with 8 threads)..." >&2
            tar --exclude="*.zip" -cf - ${final_output_dir} | pigz -p 8 > "spectronaut_output.tar.gz"
            zip_status=${PIPESTATUS[0]}
            is_targz=true
        else
            echo "Using standard zip compression..." >&2
            zip -r ${output_zip} ${final_output_dir} -x "*.zip"
            zip_status=$?
            is_targz=false
        fi

        if [ "${zip_status}" -ne 0 ]; then
            echo "ERROR: Archive creation failed with exit code ${zip_status}" >&2
            exit "${zip_status}"
        fi

        # Determine output filename based on compression method
        if [ "$is_targz" = true ]; then
            final_archive="spectronaut_output.tar.gz"
        else
            final_archive="${output_zip}"
        fi

        # Validate archive was created
        if [ ! -f "${final_archive}" ]; then
            echo "ERROR: Archive file was not created: ${final_archive}" >&2
            exit 1
        fi

        echo "=== Final output created: ${final_archive} ===" >&2
        echo "Archive size:" >&2
        ls -lh "${final_archive}" >&2

        # Test archive integrity
        echo "Testing archive integrity..." >&2
        if [ "$is_targz" = true ]; then
            if gunzip -t "${final_archive}" >&2; then
                echo "Archive integrity check passed" >&2
            else
                echo "WARNING: Archive may be corrupted" >&2
            fi
        else
            if zip -T "${final_archive}" >&2; then
                echo "Archive integrity check passed" >&2
            else
                echo "WARNING: Archive may be corrupted" >&2
            fi
        fi

        # For WDL compatibility, rename tar.gz to .zip if necessary
        if [ "$is_targz" = true ]; then
            mv "${final_archive}" "${output_zip}"
            echo "Renamed ${final_archive} to ${output_zip} for WDL compatibility" >&2
        fi
    >>>

    output {
        File output_zip = "spectronaut_output.zip"
    }

    runtime {
        docker: "broadcptacdev/panoply_spectronaut:v20.0"
        cpu: 8
        memory: "32GB"
        bootDiskSizeGb: 256
        disks: "local-disk 2000 SSD"
        preemptible: 0
    }

    meta {
        author: "C. Lian"
        email: "proteogenomics@broadinstitute.org"
    }
}

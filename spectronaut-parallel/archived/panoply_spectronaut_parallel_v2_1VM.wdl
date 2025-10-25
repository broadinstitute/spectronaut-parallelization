version development

workflow panoply_spectronaut_parallel {
    input {
        # Control flags
        Boolean do_conversion = true
        Boolean do_search = true

        # Input file selection (provide EITHER files_directory OR input_file_list)
        # If both provided, input_file_list takes priority
        Directory? files_directory          # Directory containing all input files
        File? input_file_list               # Text file with file paths (one per line)

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

        # Resource configurations for HTRMS conversion (per file)
        Int conversion_cpu_per_file = 16
        Int conversion_memory_gb_per_file = 64

        # Single-VM parallel search configuration
        Int parallel_search_jobs = 5         # Number of concurrent Spectronaut searches (4-6 recommended)
        Int search_vm_cpu = 128              # VM CPU cores (AMD Rome max)
        Int search_vm_memory_gb = 896        # VM RAM in GB (AMD Rome max)
        Int search_disk_gb_base = 500        # Base disk space for search VM

        # Per-search resource allocation (used within parallel execution)
        Int search_cpu_per_job = 25          # CPU per parallel search job (128/5 ≈ 25)
        Int search_memory_gb_per_job = 179   # RAM per parallel search job (896/5 ≈ 179)

        # Dynamic disk sizing
        Int file_size_multiplier = 3
    }

    # List files from directory if provided (and input_file_list not provided)
    if (defined(files_directory) && !defined(input_file_list)) {
        call list_files {
            input:
                file_dir = select_first([files_directory])
        }
    }

    # Determine input files (priority: input_file_list > files_directory)
    Array[File] input_files = if defined(input_file_list) then
        read_lines(select_first([input_file_list]))
    else
        select_first([list_files.input_files])

    # Step 1: Convert to HTRMS if requested (scatter over each file)
    if (do_conversion) {
        scatter (file in input_files) {
            call convert_htrms_single {
                input:
                    raw_file = file,
                    convert_schema = convert_schema,
                    cpu = conversion_cpu_per_file,
                    memory_gb = conversion_memory_gb_per_file,
                    disk_gb = ceil(size(file, "GB") * file_size_multiplier) + 20
            }
        }
    }

    # Determine which files to use for search (HTRMS if converted, otherwise raw)
    Array[File] files_to_search = select_first([convert_htrms_single.htrms_file, input_files])

    # Calculate total disk needed for parallel search VM
    Float total_files_size_gb = size(files_to_search, "GB")
    Int search_disk_gb = search_disk_gb_base + ceil(total_files_size_gb * file_size_multiplier * parallel_search_jobs / length(files_to_search))

    # Step 2: Run Spectronaut search on single VM with parallel execution
    if (do_search) {
        call spectronaut_search_parallel {
            input:
                input_files = files_to_search,
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
                parallel_jobs = parallel_search_jobs,
                cpu_per_job = search_cpu_per_job,
                memory_gb_per_job = search_memory_gb_per_job,
                cpu = search_vm_cpu,
                memory_gb = search_vm_memory_gb,
                disk_gb = search_disk_gb
        }

        # Step 3: Combine all SNE files (reports generated during combine via -rs flags)
        call combine_sne_files {
            input:
                sne_files = spectronaut_search_parallel.sne_outputs,
                experiment_name = experiment_name,
                condition_setup = condition_setup,
                fasta_1 = fasta_1,
                fasta_2 = fasta_2,
                fasta_3 = fasta_3,
                report_schema_1 = report_schema_1,
                report_schema_2 = report_schema_2
        }

        # Step 4: Zip final output
        call zip_final_output {
            input:
                combined_dir = combine_sne_files.combined_output,
                experiment_name = experiment_name
        }
    }

    output {
        File? spectronaut_output = zip_final_output.output_zip
    }

    parameter_meta {
        files_directory: "Directory containing all input files to process. Provide either this OR input_file_list."
        input_file_list: "Text file containing file paths (one per line). Takes priority if both inputs provided."
        experiment_name: "Name for the experiment, used in output file naming"
        do_conversion: "Whether to convert raw files to HTRMS format before search"
        do_search: "Whether to run Spectronaut search on files"
        parallel_search_jobs: "Number of concurrent Spectronaut searches on single VM (4-6 recommended for 128 CPU VM)"
        search_vm_cpu: "Total CPU cores for search VM (default 128 = AMD Rome max)"
        search_vm_memory_gb: "Total RAM for search VM in GB (default 896 = AMD Rome max)"
        file_size_multiplier: "Multiplier for dynamic disk sizing (disk = file_size * multiplier + base)"
    }

    meta {
        author: "C. Lian"
        email: "proteogenomics@broadinstitute.org"
        description: "Single-VM parallel Spectronaut workflow with HTRMS conversion. Uses GNU Parallel to run 4-6 concurrent Spectronaut searches on one large VM, minimizing token usage (1 token vs 4+ tokens for scatter approach). Supports two input methods: (1) Directory containing files, or (2) Text file listing file paths."
    }
}

# Task 1: List all files in input directory
task list_files {
    input {
        Directory file_dir
    }

    command <<<
        set -euo pipefail

        echo "=== Listing files in directory ===" >&2
        echo "Directory: ~{file_dir}" >&2

        # Validate directory exists
        if [ ! -d ~{file_dir} ]; then
            echo "ERROR: Directory does not exist: ~{file_dir}" >&2
            exit 1
        fi

        # Find all files (not directories) in the input directory
        find ~{file_dir} -type f > file_list.txt

        # Count the files
        file_count=$(wc -l < file_list.txt)
        echo "Found ${file_count} files to process" >&2

        # Validate that files were found
        if [ "${file_count}" -eq 0 ]; then
            echo "ERROR: No files found in directory: ~{file_dir}" >&2
            echo "Directory contents:" >&2
            ls -lhR ~{file_dir}/ >&2
            exit 1
        fi

        # Show first few files for verification
        echo "First 10 files:" >&2
        head -10 file_list.txt >&2

        # Calculate total size
        total_size=$(find ~{file_dir} -type f -exec du -ch {} + | grep total$ | cut -f1)
        echo "Total size of all files: ${total_size}" >&2

        # Output each file path on a separate line for WDL Array[File]
        cat file_list.txt
    >>>

    output {
        Array[File] input_files = read_lines(stdout())
    }

    runtime {
        docker: "ubuntu:22.04"
        cpu: 1
        memory: "2GB"
        disks: "local-disk 200 HDD"
        preemptible: 0
    }

    meta {
        author: "C. Lian"
        email: "proteogenomics@broadinstitute.org"
    }
}

# Task 2: Convert single file to HTRMS format
task convert_htrms_single {
    input {
        File raw_file
        File? convert_schema
        Int cpu
        Int memory_gb
        Int disk_gb
    }

    command <<<
        set -euo pipefail

        echo "=== Converting single file to HTRMS (broadcptacdev/panoply_spectronaut:v20.0) ===" >&2
        echo "Input file: ~{raw_file}" >&2
        echo "Working directory: $(pwd)" >&2

        # Validate input file/directory exists
        if [ ! -e ~{raw_file} ]; then
            echo "ERROR: Input file does not exist: ~{raw_file}" >&2
            exit 1
        fi

        # Get the basename and check size
        input_basename=$(basename ~{raw_file})
        echo "Processing: ${input_basename}" >&2

        if [ -d ~{raw_file} ]; then
            input_size=$(du -sh ~{raw_file} | cut -f1)
            echo "Input is a directory, size: ${input_size}" >&2
        else
            input_size=$(du -h ~{raw_file} | cut -f1)
            echo "Input is a file, size: ${input_size}" >&2
        fi

        echo "Available disk space before copy:" >&2
        df -h >&2

        # Create input and output directories
        mkdir -p input_files
        mkdir -p htrms_converted

        # Check if input is a directory or file and copy appropriately
        if [ -d ~{raw_file} ]; then
            echo "Copying directory recursively..." >&2
            if ! cp -r ~{raw_file} input_files/; then
                echo "ERROR: Failed to copy input directory" >&2
                exit 1
            fi
        else
            echo "Copying file..." >&2
            if ! cp ~{raw_file} input_files/; then
                echo "ERROR: Failed to copy input file" >&2
                exit 1
            fi
        fi

        echo "Input directory contents:" >&2
        ls -lh input_files/ >&2

        echo "Available disk space after copy:" >&2
        df -h >&2

        # Convert the file
        echo "Running spectronaut -convert..." >&2
        spectronaut -convert \
            -i input_files \
            -o htrms_converted \
            ~{if defined(convert_schema) then "-s " + convert_schema else ""} 2>&1 | tee spectronaut_convert.log >&2

        conversion_status=${PIPESTATUS[0]}
        echo "Spectronaut conversion exit status: ${conversion_status}" >&2

        if [ "${conversion_status}" -ne 0 ]; then
            echo "ERROR: Spectronaut conversion failed with exit code ${conversion_status}" >&2
            echo "=== Spectronaut output log ===" >&2
            cat spectronaut_convert.log >&2
            exit "${conversion_status}"
        fi

        echo "=== Conversion complete ===" >&2

        # List output directory contents for debugging
        echo "Output directory contents:" >&2
        ls -lhR htrms_converted/ >&2

        # Find the converted file (should have .htrms extension)
        converted_file=$(find htrms_converted -type f -name "*.htrms" | head -n 1)

        if [ -z "$converted_file" ]; then
            echo "ERROR: No .htrms file found in output directory" >&2
            echo "All files in output directory:" >&2
            find htrms_converted -type f >&2
            exit 1
        fi

        echo "Found converted file: ${converted_file}" >&2

        # Determine output filename (preserve original name with .htrms extension)
        # Strip extension from input basename and add .htrms
        output_name="${input_basename%.*}.htrms"
        echo "Expected output name: ${output_name}" >&2

        # Move to cromwell root with original name preserved
        mv "${converted_file}" "${output_name}"
        echo "Output file created: ${output_name}" >&2
        ls -lh "${output_name}" >&2
    >>>

    output {
        File htrms_file = sub(basename(raw_file), "\\.(raw|htrms|RAW|HTRMS)$", "") + ".htrms"
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
        raw_file: {
            stream: true,
            description: "Raw mass spectrometry file for conversion (streaming enabled for large files)"
        }
    }

    meta {
        author: "C. Lian"
        email: "proteogenomics@broadinstitute.org"
    }
}

# Task 3: Run Spectronaut search on all files in parallel on single VM
task spectronaut_search_parallel {
    input {
        Array[File] input_files
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

        Int parallel_jobs         # Number of concurrent searches
        Int cpu_per_job           # CPU per individual search job
        Int memory_gb_per_job     # RAM per individual search job
        Int cpu                   # Total VM CPU
        Int memory_gb             # Total VM RAM
        Int disk_gb               # Total VM disk
    }

    command <<<
        set -euo pipefail

        echo "=== Spectronaut parallel search on single VM (broadcptacdev/panoply_spectronaut:v20.0) ===" >&2
        echo "Total files to process: ~{length(input_files)}" >&2
        echo "Parallel jobs: ~{parallel_jobs}" >&2
        echo "VM resources: ~{cpu} CPU, ~{memory_gb}GB RAM, ~{disk_gb}GB disk" >&2
        echo "Per-job allocation: ~{cpu_per_job} CPU, ~{memory_gb_per_job}GB RAM" >&2
        echo "DirectDIA mode: ~{if !defined(spectral_library_1) then "true" else "false"}" >&2
        echo "" >&2

        # Create output and log directories
        mkdir -p outputs logs

        # Write all input files to a list
        cat > files.txt <<'EOF'
~{sep('\n', input_files)}
EOF

        file_count=$(wc -l < files.txt)
        echo "File list created with ${file_count} files" >&2
        echo "First 5 files:" >&2
        head -5 files.txt >&2
        echo "" >&2

        echo "Available resources before starting:" >&2
        echo "Disk space:" >&2
        df -h >&2
        echo "Memory:" >&2
        free -h >&2
        echo "" >&2

        # Import enzyme database once if provided
        ~{if defined(enzyme_database) then "echo 'Importing enzyme database...' >&2\ndotnet /usr/lib/spectronaut/SpectronautCMD.dll --importEnzymeDB " + enzyme_database + "\necho 'Enzyme database imported' >&2\necho '' >&2" else ""}

        # Define search function for one file
        search_one_file() {
            local input_file="$1"
            local job_id="$2"

            local input_basename=$(basename "$input_file")
            local file_exp_name="~{experiment_name}_${input_basename}"

            echo "[Job ${job_id}] Starting search for: ${input_basename}" >&2

            # Create unique temp directories for this job
            local cromwell_root=$(pwd)
            local sn_temp="${cromwell_root}/sn_temp_${job_id}"
            local work_dir="${cromwell_root}/work_${job_id}"

            mkdir -p "${work_dir}/data" "${work_dir}/spectronaut_out" "${sn_temp}"

            # Copy input file to work directory
            if ! cp "$input_file" "${work_dir}/data/"; then
                echo "[Job ${job_id}] ERROR: Failed to copy input file" >&2
                rm -rf "${work_dir}" "${sn_temp}"
                return 1
            fi

            echo "[Job ${job_id}] Running Spectronaut search..." >&2

            # Run Spectronaut search
            if ! spectronaut \
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
                -o "${work_dir}/spectronaut_out" \
                -fasta ~{fasta_1} \
                -d "${work_dir}/data" \
                -setTemp "${sn_temp}" > "${cromwell_root}/logs/${input_basename}.log" 2>&1; then

                echo "[Job ${job_id}] ERROR: Spectronaut search failed for ${input_basename}" >&2
                echo "[Job ${job_id}] See log: logs/${input_basename}.log" >&2
                rm -rf "${work_dir}" "${sn_temp}"
                return 1
            fi

            # Find and move SNE file
            local sne_file=$(find "${work_dir}/spectronaut_out" -type f -name "*.sne" | head -n 1)

            if [ -z "$sne_file" ]; then
                echo "[Job ${job_id}] ERROR: No .sne file found for ${input_basename}" >&2
                rm -rf "${work_dir}" "${sn_temp}"
                return 1
            fi

            local out_name="${file_exp_name}.sne"
            mv "$sne_file" "${cromwell_root}/outputs/${out_name}"

            if [ ! -f "${cromwell_root}/outputs/${out_name}" ]; then
                echo "[Job ${job_id}] ERROR: Failed to move SNE file" >&2
                rm -rf "${work_dir}" "${sn_temp}"
                return 1
            fi

            local sne_size=$(du -h "${cromwell_root}/outputs/${out_name}" | cut -f1)
            echo "[Job ${job_id}] ✓ Completed: ${out_name} (${sne_size})" >&2

            # Cleanup working directories
            rm -rf "${work_dir}" "${sn_temp}"

            return 0
        }

        # Export function for GNU Parallel
        export -f search_one_file

        # Run parallel searches using GNU Parallel
        echo "Starting parallel execution with ~{parallel_jobs} concurrent jobs..." >&2
        echo "" >&2

        cat files.txt | parallel \
            --jobs ~{parallel_jobs} \
            --joblog joblog.txt \
            --line-buffer \
            --halt soon,fail=20% \
            --timeout 0 \
            search_one_file {} {#}

        parallel_exit=$?

        echo "" >&2
        echo "=== Parallel execution complete ===" >&2
        echo "GNU Parallel exit code: ${parallel_exit}" >&2
        echo "" >&2

        # Count results
        sne_count=$(find outputs -name "*.sne" -type f 2>/dev/null | wc -l)
        echo "Total SNE files created: ${sne_count} / ${file_count}" >&2
        echo "" >&2

        # Show job log summary
        if [ -f joblog.txt ]; then
            echo "Job execution summary:" >&2
            column -t joblog.txt >&2
            echo "" >&2
        fi

        # Verify we have outputs
        if [ "${sne_count}" -eq 0 ]; then
            echo "ERROR: No SNE files were created" >&2
            echo "All jobs failed. Check individual log files in logs/ directory" >&2
            exit 1
        fi

        # Show successful outputs
        echo "Successfully created SNE files:" >&2
        ls -lh outputs/ >&2
        echo "" >&2

        # Check for failures
        failed_count=$((file_count - sne_count))
        if [ "${failed_count}" -gt 0 ]; then
            echo "WARNING: ${failed_count} files failed to process" >&2
            echo "Check logs/ directory for individual job logs" >&2
        fi

        # Final resource check
        echo "Final disk space:" >&2
        df -h >&2
        echo "" >&2
        echo "Final memory:" >&2
        free -h >&2
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

    meta {
        author: "C. Lian"
        email: "proteogenomics@broadinstitute.org"
        description: "Runs multiple Spectronaut searches in parallel on a single large VM using GNU Parallel. Minimizes token usage while maintaining high throughput."
    }
}

# Task 4: Combine all SNE files into one
task combine_sne_files {
    input {
        Array[File] sne_files
        String experiment_name

        File? condition_setup
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
            echo "This usually means:" >&2
            echo "  1. All search jobs failed to produce SNE files" >&2
            echo "  2. The search task did not complete successfully" >&2
            echo "  3. No input files were provided" >&2
            exit 1
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
        eval spectronaut -combine \
            -n ~{experiment_name} \
            -o ${out_dir} \
            ${sne_args} \
            -fasta ~{fasta_1} \
            ~{if defined(fasta_2) then "-fasta " + fasta_2 else ""} \
            ~{if defined(fasta_3) then "-fasta " + fasta_3 else ""} \
            ~{if defined(condition_setup) then "-con " + condition_setup else ""} \
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
        disks: "local-disk 2000 HDD"
        preemptible: 0
    }

    meta {
        author: "C. Lian"
        email: "proteogenomics@broadinstitute.org"
    }
}

# Task 5: Zip final output (combined SNE and reports)
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

        # Create zip file, excluding any nested zip files
        output_zip="spectronaut_output.zip"
        echo "Creating zip archive..." >&2
        zip -r ${output_zip} ${final_output_dir} -x "*.zip"

        zip_status=$?
        if [ "${zip_status}" -ne 0 ]; then
            echo "ERROR: Zip creation failed with exit code ${zip_status}" >&2
            exit "${zip_status}"
        fi

        # Validate zip was created
        if [ ! -f "${output_zip}" ]; then
            echo "ERROR: Zip file was not created" >&2
            exit 1
        fi

        echo "=== Final output created: ${output_zip} ===" >&2
        echo "Archive size:" >&2
        ls -lh "${output_zip}" >&2

        # Test zip integrity
        echo "Testing zip integrity..." >&2
        if zip -T "${output_zip}" >&2; then
            echo "Zip file integrity check passed" >&2
        else
            echo "WARNING: Zip file may be corrupted" >&2
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
        disks: "local-disk 2000 HDD"
        preemptible: 0
    }

    meta {
        author: "C. Lian"
        email: "proteogenomics@broadinstitute.org"
    }
}

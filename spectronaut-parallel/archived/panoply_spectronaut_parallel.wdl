version development

workflow panoply_spectronaut_parallel {
    input {
        # Control flags
        Boolean do_conversion = true
        Boolean do_search = true

        # Input data directory
        Directory file_dir

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

        # Resource configurations per file (sufficient for most cases)
        Int conversion_cpu_per_file = 16
        Int conversion_memory_gb_per_file = 128
        Int conversion_disk_gb_per_file = 60

        Int search_cpu_per_file = 32
        Int search_memory_gb_per_file = 512
        Int search_disk_gb_per_file = 60

        # Combination and reporting resources
        Int combine_cpu = 32
        Int combine_memory_gb = 512
        Int combine_disk_gb = 1000

        Int report_cpu = 16
        Int report_memory_gb = 256
        Int report_disk_gb = 500
    }

    # Step 1: List all input files for parallel processing
    call list_files {
        input:
            file_dir = file_dir
    }

    # Step 2: Convert to HTRMS if requested (scatter over each file)
    if (do_conversion) {
        scatter (file in list_files.input_files) {
            call convert_htrms_single {
                input:
                    raw_file = file,
                    convert_schema = convert_schema,
                    cpu = conversion_cpu_per_file,
                    memory_gb = conversion_memory_gb_per_file,
                    disk_gb = conversion_disk_gb_per_file
            }
        }
    }

    # Determine which files to use for search (HTRMS if converted, otherwise raw)
    Array[File] files_to_search = select_first([convert_htrms_single.htrms_file, list_files.input_files])

    # Step 3: Run Spectronaut search on each file (scatter)
    if (do_search) {
        scatter (file in files_to_search) {
            call spectronaut_search_single {
                input:
                    input_file = file,
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
                    cpu = search_cpu_per_file,
                    memory_gb = search_memory_gb_per_file,
                    disk_gb = search_disk_gb_per_file
            }
        }

        # Step 4: Combine all SNE files
        call combine_sne_files {
            input:
                sne_files = spectronaut_search_single.sne_output,
                experiment_name = experiment_name,
                condition_setup = condition_setup,
                fasta_1 = fasta_1,
                fasta_2 = fasta_2,
                fasta_3 = fasta_3,
                report_schema_1 = report_schema_1,
                report_schema_2 = report_schema_2,
                cpu = combine_cpu,
                memory_gb = combine_memory_gb,
                disk_gb = combine_disk_gb
        }

        # Step 5: Generate reports if schemas provided (combine may not generate all reports)
        if (defined(report_schema_1) || defined(report_schema_2)) {
            call generate_reports {
                input:
                    combined_dir = combine_sne_files.combined_output,
                    report_schema_1 = report_schema_1,
                    report_schema_2 = report_schema_2,
                    cpu = report_cpu,
                    memory_gb = report_memory_gb,
                    disk_gb = report_disk_gb
            }
        }

        # Step 6: Zip final output
        call zip_final_output {
            input:
                combined_dir = combine_sne_files.combined_output,
                reports_dir = generate_reports.reports_output,
                experiment_name = experiment_name
        }
    }

    output {
        File? spectronaut_output = zip_final_output.output_zip
        Int num_files_processed = list_files.file_count
    }

    meta {
        author: "C. Lian"
        email: "proteogenomics@broadinstitute.org"
        description: "Parallel Spectronaut workflow with HTRMS conversion: Files are converted and searched individually, and SNE results are combined eventually."
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

        # Find all files (not directories) in the input directory
        find ~{file_dir} -type f > file_list.txt

        # Count the files
        file_count=$(wc -l < file_list.txt)
        echo "Found ${file_count} files to process" >&2
        echo "${file_count}" > file_count.txt

        # Output each file path on a separate line for WDL Array[File]
        cat file_list.txt
    >>>

    output {
        Array[File] input_files = read_lines(stdout())
        Int file_count = read_int("file_count.txt")
    }

    runtime {
        docker: "ubuntu:22.04"
        cpu: 1
        memory: "2GB"
        disks: "local-disk 200 HDD"
        preemptible: 0
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

        # Create output directory
        mkdir -p htrms_converted

        # Get the basename of the input file
        input_basename=$(basename ~{raw_file})
        echo "Processing: ${input_basename}" >&2

        # Create a temporary directory and copy the single file
        temp_input_dir=$(mktemp -d)
        cp ~{raw_file} "${temp_input_dir}"/

        # Convert the file
        spectronaut -convert \
            -i "${temp_input_dir}" \
            -o htrms_converted \
            ~{if defined(convert_schema) then "-s " + convert_schema else ""}

        echo "=== Conversion complete ===" >&2

        # Find the converted file (should have .htrms extension)
        converted_file=$(find htrms_converted -type f -name "*.htrms" | head -n 1)

        if [ -z "$converted_file" ]; then
            echo "ERROR: No .htrms file found in output directory" >&2
            exit 1
        fi

        # Move to a standard output location
        mv "${converted_file}" htrms_output.htrms
        echo "Output file: htrms_output.htrms" >&2
    >>>

    output {
        File htrms_file = "htrms_output.htrms"
    }

    runtime {
        docker: "broadcptacdev/panoply_spectronaut:v20.0"
        cpuPlatform: "AMD Rome"
        cpu: cpu
        memory: "~{memory_gb}GB"
        bootDiskSizeGb: 512
        disks: "local-disk ~{disk_gb} HDD"
        preemptible: 0
    }

    meta {
        author: "C. Lian"
        email: "proteogenomics@broadinstitute.org"
    }
}

# Task 3: Run Spectronaut search on single file, output only .sne file
task spectronaut_search_single {
    input {
        File input_file
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

        # Get basename for unique experiment name
        input_basename=$(basename ~{input_file})
        file_exp_name="~{experiment_name}_${input_basename}"

        echo "=== Spectronaut search for single file (broadcptacdev/panoply_spectronaut:v20.0) ===" >&2
        echo "Input file: ${input_basename}" >&2
        echo "Experiment name: ${file_exp_name}" >&2
        echo "DirectDIA mode: ~{if !defined(spectral_library_1) then "true" else "false"}" >&2

        # Set up working directory structure (following original WDL pattern)
        cromwell_root=$(pwd)
        sn_temp=$(mktemp -d sn_temp_XXXXXX)
        working_dir=$(mktemp -d working_dir_XXXXXX)
        cd "${working_dir}"

        out_dir="spectronaut_out"
        mkdir -p ${out_dir}

        # Create data directory with single file
        mkdir -p data
        cp ~{input_file} data/

        echo "=== Running Spectronaut ===" >&2

        # Import enzyme database if provided
        ~{if defined(enzyme_database) then "dotnet /usr/lib/spectronaut/SpectronautCMD.dll --importEnzymeDB " + enzyme_database else ""}

        # Run Spectronaut search
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
            -o ${out_dir} \
            -fasta ~{fasta_1} \
            -d data \
            -setTemp "${sn_temp}"

        echo "=== Spectronaut search complete ===" >&2

        # Find and extract only the .sne file
        sne_file=$(find ${out_dir} -type f -name "*.sne" | head -n 1)

        if [ -z "$sne_file" ]; then
            echo "ERROR: No .sne file found in output directory" >&2
            exit 1
        fi

        # Copy SNE file to cromwell root with standardized name
        cp "${sne_file}" "${cromwell_root}"/output.sne
        echo "SNE output: output.sne" >&2
    >>>

    output {
        File sne_output = "output.sne"
    }

    runtime {
        docker: "broadcptacdev/panoply_spectronaut:v20.0"
        cpuPlatform: "AMD Rome"
        cpu: cpu
        memory: "~{memory_gb}GB"
        bootDiskSizeGb: 512
        disks: "local-disk ~{disk_gb} HDD"
        preemptible: 0
    }

    meta {
        author: "C. Lian"
        email: "proteogenomics@broadinstitute.org"
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

        Int cpu
        Int memory_gb
        Int disk_gb
    }

    command <<<
        set -euo pipefail

        echo "=== Combining SNE files (broadcptacdev/panoply_spectronaut:v20.0) ===" >&2
        echo "Number of SNE files: ~{length(sne_files)}" >&2

        out_dir="combined_output"
        mkdir -p ${out_dir}

        # Build the spectronaut combine command
        echo "Running SNE combination..." >&2

        # Execute combine command
        spectronaut -combine \
            -n ~{experiment_name} \
            -o ${out_dir} \
            ~{sep(" ", prefix("-sne ", sne_files))} \
            -fasta ~{fasta_1} \
            ~{if defined(fasta_2) then "-fasta " + fasta_2 else ""} \
            ~{if defined(fasta_3) then "-fasta " + fasta_3 else ""} \
            ~{if defined(condition_setup) then "-con " + condition_setup else ""} \
            ~{if defined(report_schema_1) then "-rs " + report_schema_1 else ""} \
            ~{if defined(report_schema_2) then "-rs " + report_schema_2 else ""}

        echo "=== SNE combination complete ===" >&2

        # List output contents
        echo "Output directory contents:" >&2
        ls -lh ${out_dir}/ >&2
    >>>

    output {
        Directory combined_output = "combined_output"
    }

    runtime {
        docker: "broadcptacdev/panoply_spectronaut:v20.0"
        cpuPlatform: "AMD Rome"
        cpu: cpu
        memory: "~{memory_gb}GB"
        bootDiskSizeGb: 512
        disks: "local-disk ~{disk_gb} HDD"
        preemptible: 0
    }

    meta {
        author: "C. Lian"
        email: "proteogenomics@broadinstitute.org"
    }
}

# Task 5: Generate reports from combined SNE (if not already generated)
task generate_reports {
    input {
        Directory combined_dir
        File? report_schema_1
        File? report_schema_2

        Int cpu
        Int memory_gb
        Int disk_gb
    }

    command <<<
        set -euo pipefail

        echo "=== Generating reports from combined SNE (broadcptacdev/panoply_spectronaut:v20.0) ===" >&2

        # Find the combined SNE file
        sne_file=$(find ~{combined_dir} -type f -name "*.sne" | head -n 1)

        if [ -z "$sne_file" ]; then
            echo "ERROR: No .sne file found in combined directory" >&2
            exit 1
        fi

        echo "Using SNE file: ${sne_file}" >&2

        out_dir="reports_output"
        mkdir -p ${out_dir}

        # Generate reports using manageSNE
        spectronaut manageSNE \
            -sne "${sne_file}" \
            -o ${out_dir} \
            ~{if defined(report_schema_1) then "-rs " + report_schema_1 else ""} \
            ~{if defined(report_schema_2) then "-rs " + report_schema_2 else ""}

        echo "=== Report generation complete ===" >&2

        # List generated reports
        echo "Generated reports:" >&2
        ls -lh ${out_dir}/ >&2
    >>>

    output {
        Directory reports_output = "reports_output"
    }

    runtime {
        docker: "broadcptacdev/panoply_spectronaut:v20.0"
        cpuPlatform: "AMD Rome"
        cpu: cpu
        memory: "~{memory_gb}GB"
        bootDiskSizeGb: 512
        disks: "local-disk ~{disk_gb} HDD"
        preemptible: 0
    }

    meta {
        author: "C. Lian"
        email: "proteogenomics@broadinstitute.org"
    }
}

# Task 6: Zip final output (combined SNE and reports)
task zip_final_output {
    input {
        Directory combined_dir
        Directory? reports_dir
        String experiment_name
    }

    command <<<
        set -euo pipefail

        echo "=== Creating final output zip ===" >&2

        final_output_dir="~{experiment_name}_output"
        mkdir -p ${final_output_dir}

        # Copy combined SNE results
        echo "Copying combined SNE results..." >&2
        cp -r ~{combined_dir}/* ${final_output_dir}/

        # Copy reports if they exist
        ~{if defined(reports_dir) then
            "echo \"Copying generated reports...\" >&2\ncp -r ~{reports_dir}/* ~{experiment_name}_output/"
            else ""}

        # Create zip file, excluding any nested zip files
        output_zip="spectronaut_output.zip"
        echo "Creating zip archive..." >&2
        zip -r ${output_zip} ${final_output_dir} -x "*.zip"

        echo "=== Final output created: ${output_zip} ===" >&2
        echo "Archive size:" >&2
        ls -lh ${output_zip} >&2
    >>>

    output {
        File output_zip = "spectronaut_output.zip"
    }

    runtime {
        docker: "broadcptacdev/panoply_spectronaut:v20.0"
        cpu: 8
        memory: "32GB"
        bootDiskSizeGb: 256
        disks: "local-disk 500 HDD"
        preemptible: 0
    }

    meta {
        author: "C. Lian"
        email: "proteogenomics@broadinstitute.org"
    }
}

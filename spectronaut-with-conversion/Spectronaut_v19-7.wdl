version development

workflow panoply_spectronaut {
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

    # Resource configurations per file
    Int conversion_cpu_per_file = 8
    Int conversion_memory_gb_per_file = 32

    Int search_cpu = 32
    Int search_memory_gb = 256

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
  # read_lines returns Array[String] which WDL coerces to Array[File]
  Array[File]? files_from_list_file = if defined(input_file_list) then read_lines(select_first([input_file_list])) else None
  Array[File] input_files = select_first([files_from_list_file, list_files.input_files])

  # Note: Workflow will fail if neither files_directory nor input_file_list is provided
  # This is enforced by select_first failing when list_files was not called

  # Step 1: Convert to HTRMS if requested (scatter over each file - 1 VM per file)
  if (do_conversion) {
    scatter (file in input_files) {
      call convert_htrms_single {
        input:
          raw_file = file,
          convert_schema = convert_schema,
          cpu = conversion_cpu_per_file,
          memory_gb = conversion_memory_gb_per_file,
          disk_gb = 100 + ceil(size(file, "GB") * file_size_multiplier)
      }
    }
  }

  # Determine which files to use for search (HTRMS if converted, otherwise raw)
  Array[File] files_to_search = select_first([convert_htrms_single.htrms_file, input_files])

  # Calculate disk for search VM based on total file sizes
  Float total_files_size_gb = size(files_to_search, "GB")
  Int search_disk_gb = 200 + ceil(total_files_size_gb * file_size_multiplier)

  # Step 2: Run Spectronaut search (single VM, sequential processing)
  if (do_search) {
    call spectronaut_search {
      input:
        input_files = files_to_search,
        experiment_name = experiment_name,
        analysis_settings = analysis_settings,
        condition_setup = condition_setup,
        fasta_1 = fasta_1,
        fasta_2 = fasta_2,
        fasta_3 = fasta_3,
        enzyme_database = enzyme_database,
        spectral_library_1 = spectral_library_1,
        spectral_library_2 = spectral_library_2,
        report_schema_1 = report_schema_1,
        report_schema_2 = report_schema_2,
        json_settings = json_settings,
        cpu = search_cpu,
        memory_gb = search_memory_gb,
        disk_gb = search_disk_gb
    }
  }

  output {
    File? spectronaut_output = spectronaut_search.spectronaut_output
  }

  parameter_meta {
    files_directory: "Directory containing all input files to process. Provide either this OR input_file_list."
    input_file_list: "Text file containing file paths (one per line). Takes priority if both inputs provided."
    experiment_name: "Name for the experiment, used in output file naming"
    do_conversion: "Whether to convert raw files to HTRMS format before search"
    do_search: "Whether to run Spectronaut search on files"
    file_size_multiplier: "Multiplier for dynamic disk sizing. Conversion: disk = file_size * multiplier + 100GB. Search: disk = total_size * multiplier + 200GB."
  }

  meta {
    author: "C. Lian"
    email: "proteogenomics@broadinstitute.org"
    description: "Spectronaut v19.7 workflow with HTRMS conversion. Supports two input methods: (1) Directory containing files, or (2) Text file listing file paths. HTRMS conversion is scattered (1 VM per file). Spectronaut search runs sequentially on a single VM."
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
    disks: "local-disk 200 SSD"
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

    echo "=== Converting single file to HTRMS (broadcptacdev/panoply_spectronaut:v19.7) ===" >&2
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
    docker: "broadcptacdev/panoply_spectronaut:v19.7"
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

# Task 3: Run Spectronaut search sequentially on all files (single VM)
task spectronaut_search {
  input {
    Array[File] input_files
    String experiment_name

    File? analysis_settings
    File? condition_setup

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

    echo "=== Spectronaut search (broadcptacdev/panoply_spectronaut:v19.7) ===" >&2
    echo "Experiment name: ~{experiment_name}" >&2
    echo "DirectDIA mode: ~{if !defined(spectral_library_1) then "true" else "false"}" >&2
    echo "Number of files: ~{length(input_files)}" >&2
    echo "" >&2

    # Create localized files list
    cat > all_files.txt <<'EOF'
~{sep('\n', input_files)}
EOF

    echo "Input files to process:" >&2
    cat all_files.txt >&2
    echo "" >&2

    # Setup directories
    cromwell_root=$(pwd)
    echo "Cromwell root: ${cromwell_root}" >&2

    out_dir="spectronaut_out"
    mkdir -p ${out_dir}

    sn_temp=$(mktemp -d sn_temp_XXXXXX)
    data_dir=$(mktemp -d data_XXXXXX)

    echo "Available resources:" >&2
    echo "Disk space:" >&2
    df -h >&2
    echo "Memory:" >&2
    free -h >&2
    echo "" >&2

    # Copy all input files to data directory
    echo "Copying input files to data directory..." >&2
    while IFS= read -r file_path; do
      if [ ! -e "$file_path" ]; then
        echo "ERROR: Input file does not exist: $file_path" >&2
        exit 1
      fi
      cp "$file_path" "${data_dir}/"
      echo "  Copied: $(basename "$file_path")" >&2
    done < all_files.txt

    echo "" >&2
    echo "Data directory contents:" >&2
    ls -lh "${data_dir}/" >&2
    echo "" >&2

    # Import enzyme database if provided
    ~{if defined(enzyme_database) then "echo 'Importing enzyme database...' >&2\ndotnet /usr/lib/spectronaut/SpectronautCMD.dll --importEnzymeDB " + enzyme_database + "\necho 'Enzyme database imported' >&2\necho '' >&2" else ""}

    # Run Spectronaut search
    echo "Running Spectronaut search..." >&2
    spectronaut \
      ~{if !defined(spectral_library_1) then "-direct" else ""} \
      ~{if defined(analysis_settings) then "-s " + analysis_settings else ""} \
      ~{if defined(condition_setup) then "-con " + condition_setup else ""} \
      ~{if defined(fasta_2) then "-fasta " + fasta_2 else ""} \
      ~{if defined(fasta_3) then "-fasta " + fasta_3 else ""} \
      ~{if defined(spectral_library_1) then "-a " + spectral_library_1 else ""} \
      ~{if defined(spectral_library_2) then "-a " + spectral_library_2 else ""} \
      ~{if defined(report_schema_1) then "-rs " + report_schema_1 else ""} \
      ~{if defined(report_schema_2) then "-rs " + report_schema_2 else ""} \
      ~{if defined(json_settings) then "-j " + json_settings else ""} \
      -n ~{experiment_name} \
      -o "${out_dir}" \
      -fasta ~{fasta_1} \
      -d "${data_dir}" \
      -setTemp "${sn_temp}" 2>&1 | tee spectronaut_search.log >&2

    search_status=${PIPESTATUS[0]}
    echo "Spectronaut search exit status: ${search_status}" >&2

    if [ "${search_status}" -ne 0 ]; then
      echo "ERROR: Spectronaut search failed with exit code ${search_status}" >&2
      echo "=== Spectronaut output log ===" >&2
      cat spectronaut_search.log >&2
      exit "${search_status}"
    fi

    echo "" >&2
    echo "=== Spectronaut search complete ===" >&2

    # List output directory contents
    echo "Output directory contents:" >&2
    ls -lhR ${out_dir}/ >&2
    echo "" >&2

    # Create zip archive with parallel compression if available
    output_zip="spectronaut_output.zip"
    echo "Creating output archive..." >&2

    if command -v pigz &> /dev/null; then
      echo "Using parallel compression (tar + pigz with 8 threads)..." >&2
      tar --exclude="*.zip" -cf - ${out_dir} | pigz -p 8 > "spectronaut_output.tar.gz"
      zip_status=${PIPESTATUS[0]}
      is_targz=true
    else
      echo "Using standard zip compression..." >&2
      zip -r ${output_zip} ${out_dir} -x "*.zip"
      zip_status=$?
      is_targz=false
    fi

    if [ "${zip_status}" -ne 0 ]; then
      echo "ERROR: Archive creation failed with exit code ${zip_status}" >&2
      exit "${zip_status}"
    fi

    # Determine output filename
    if [ "$is_targz" = true ]; then
      final_archive="spectronaut_output.tar.gz"
    else
      final_archive="${output_zip}"
    fi

    # Validate archive
    if [ ! -f "${final_archive}" ]; then
      echo "ERROR: Archive file was not created: ${final_archive}" >&2
      exit 1
    fi

    echo "Archive created: ${final_archive}" >&2
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

    # Rename tar.gz to .zip for WDL compatibility
    if [ "$is_targz" = true ]; then
      mv "${final_archive}" "${output_zip}"
      echo "Renamed ${final_archive} to ${output_zip} for WDL compatibility" >&2
    fi

    echo "" >&2
    echo "=== Final output created: ${output_zip} ===" >&2
  >>>

  output {
    File spectronaut_output = "spectronaut_output.zip"
  }

  runtime {
    docker: "broadcptacdev/panoply_spectronaut:v19.7"
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
  }
}

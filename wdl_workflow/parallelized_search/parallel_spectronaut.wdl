version development

# Two-Step Pulsar Search Architecture with Optimized Model Training
# Phase I: Discovery - List raw files from GCS
# Phase II: Intelligent Binning - Distribute files across n VMs
# Phase III: Conditional Execution - Single VM (num_vms=1) or Parallel Multi-Step (num_vms>1)
#   Parallel Mode:
#     Step 1.1: Generate intermediate search archives per bin
#     Step 1.2: Combine archives to train optimized models (.qsp)
#     Step 1.3: Generate final search archives with optimized models
#     Step 1.4: Merge final archives into search library (.kit)
#     Step 2: DIA analysis per bin against merged library
#     Step 3: Combine SNE files and generate reports
#   Single VM Mode:
#     DirectDIA search and analysis in one step
#   All Spectronaut tasks (except HTRMS conversion) use directDIA_settings
workflow parallel_spectronaut {
    input {
        File fasta_1  # Primary FASTA database file (required)

        # ============================================================================
        # Analysis Settings & Schemas
        # ============================================================================
        File directDIA_settings  # Settings file for all Spectronaut tasks (except HTRMS conversion)
        # ============================================================================
        # Required Workflow Parameters
        # ============================================================================
        String experiment_name  # Experiment identifier
        String file_directory  # GCS path to raw input files

        # ============================================================================
        # Optional Database Files
        # ============================================================================
        File? fasta_2  # Additional FASTA database file
        File? fasta_3  # Additional FASTA database file
        File? enzyme_database  # Custom enzyme database
        File? custom_mod_repository  # Custom modification repository imported via --importModRepository
        File? json_settings  # JSON settings for Spectronaut
        File? condition_setup  # Experimental condition setup

        # ============================================================================
        # Report Schemas
        # ============================================================================
        File? report_schema_1  # Report schema 1
        File? report_schema_2  # Report schema 2
        File? report_schema_3  # Report schema 3
        File? report_schema_4  # Report schema 4
        File? convert_schema  # Optional schema for HTRMS conversion

        # ============================================================================
        # Workflow Configuration
        # ============================================================================
        String experiment_type = "proteome"  # Experiment type: "proteome" or "ptm" (affects resource presets)

        # ============================================================================
        # HTRMS Conversion Configuration
        # ============================================================================
        Boolean do_conversion = false  # Enable HTRMS file conversion (default: false)

        # ============================================================================
        # Disk Sizing Configuration
        # ============================================================================
        Float disk_size_multiplier = 3  # Multiplier for disk size calculation (default: 3)
        Float sne_combine_disk_size_multiplier = 6  # Separate disk size multiplier for SNE combine step (default: 6)
        Float average_file_size_gb = 20  # Average file size in GB for disk allocation (default: 20)
        Int num_vms = 1  # Number of VMs for parallel processing (1 = single VM, >1 = parallel)

        # ============================================================================
        # Resource Configuration
        # ============================================================================

        # Preemptible instance settings (0 = non-preemptible, >0 = number of preemptible attempts)
        Int n_preemptible_pulsar_step1 = 1  # Pulsar step 1 preemptible attempts
        Int n_preemptible_pulsar_step2 = 0  # Pulsar step 2 preemptible attempts
        Int n_preemptible_pulsar_step3 = 1  # Pulsar step 3 preemptible attempts
        Int n_preemptible_directDIA_single_vm = 0  # DirectDIA single VM preemptible attempts
        Int n_preemptible_combine_archives = 0  # Archive combining preemptible attempts
        Int n_preemptible_dia_analysis = 1  # DIA analysis preemptible attempts
        Int n_preemptible_combine_sne = 0  # SNE combining preemptible attempts
        Int n_preemptible_htrms_conversion = 2  # Preemptible attempts for conversion
        Boolean generate_sne_large_experiment = true  # If true, use manageSNE --merge; if false, use spectronaut combine

        # ============================================================================
        # Spectral Library Bypass Configuration
        # ============================================================================
        Boolean do_pulsar = true  # Run Pulsar steps; set to false to skip Pulsar and use user-provided libraries directly
        String? spectral_library_1   # Optional user-provided spectral library (.kit file, GCS path)
        String? spectral_library_2   # Optional user-provided spectral library (.kit file, GCS path)
        String? spectral_library_3   # Optional user-provided spectral library (.kit file, GCS path)
    }

    # Compute preset configurations based on experiment_type
    # Pulsar Step 1: Generate intermediate search archives (similar to directDIA search)
    Map[String, Int] pulsar_step1_cpu_presets = {
        "proteome": 50,
        "ptm": 64,
    }
    # Absolute RAM allocation (GB)
    Map[String, Int] pulsar_step1_ram_gb_presets = {
        "proteome": 128,
        "ptm": 156,
    }

    # Pulsar Step 2: Combine intermediate archives and train models (memory-intensive)
    Map[String, Int] pulsar_step2_cpu_presets = {
        "proteome": 25,
        "ptm": 32,
    }
    # Base RAM per total file (GB/file) for dynamic scaling
    Map[String, Float] pulsar_step2_base_ram_per_file_presets = {
        "proteome": 1.0,
        "ptm": 1.2,
    }

    # Pulsar Step 3: Generate final archives with optimized models (compute-intensive)
    Map[String, Int] pulsar_step3_cpu_presets = {
        "proteome": 40,
        "ptm": 50,
    }
    # Absolute RAM allocation (GB)
    Map[String, Int] pulsar_step3_ram_gb_presets = {
        "proteome": 96,
        "ptm": 128,
    }

    # Combine final archives into .kit library
    Map[String, Int] combine_archives_cpu_presets = {
        "proteome": 25,
        "ptm": 32,
    }
    # Base RAM per total file (GB/file) for dynamic scaling
    Map[String, Float] combine_archives_base_ram_per_file_presets = {
        "proteome": 1.0,
        "ptm": 1.2,
    }

    # DirectDIA single VM (does both search and analysis in one step)
    Map[String, Int] directDIA_single_vm_cpu_presets = {
        "proteome": 64,
        "ptm": 128,
    }
    Map[String, Int] directDIA_single_vm_ram_gb_presets = {
        "proteome": 128,
        "ptm": 256,
    }

    # DIA analysis per bin (after library is created)
    Map[String, Int] dia_analysis_cpu_presets = {
        "proteome": 35,
        "ptm": 45,
    }
    # Absolute RAM allocation (GB)
    Map[String, Int] dia_analysis_ram_gb_presets = {
        "proteome": 96,
        "ptm": 128,
    }

    Map[String, Int] combine_sne_cpu_presets = {
        "proteome": 25,
        "ptm": 30,
    }
    # Base RAM per total file (GB/file) for dynamic scaling — manageSNE --merge mode
    Map[String, Float] combine_sne_ram_per_file_merge_presets = {
        "proteome": 5.0,
        "ptm": 6.0,
    }
    # Base RAM per total file (GB/file) for dynamic scaling — spectronaut combine mode
    Map[String, Float] combine_sne_ram_per_file_combine_presets = {
        "proteome": 3.0,
        "ptm": 4.0,
    }

    # Validate experiment_type and fallback to "proteome" if invalid
    String validated_experiment_type = if (experiment_type == "proteome" || experiment_type
        == "ptm") then experiment_type else "proteome"

    # Derived booleans for spectral library bypass logic
    Boolean has_library = defined(spectral_library_1) || defined(spectral_library_2) || defined(spectral_library_3)
    Boolean run_pulsar = do_pulsar

    # Look up values based on validated_experiment_type
    Int pulsar_step1_cpu = pulsar_step1_cpu_presets[validated_experiment_type]
    Int pulsar_step1_ram_gb = pulsar_step1_ram_gb_presets[validated_experiment_type]

    Int pulsar_step2_cpu = pulsar_step2_cpu_presets[validated_experiment_type]
    Float pulsar_step2_base_ram_per_file = pulsar_step2_base_ram_per_file_presets[validated_experiment_type]

    Int pulsar_step3_cpu = pulsar_step3_cpu_presets[validated_experiment_type]
    Int pulsar_step3_ram_gb = pulsar_step3_ram_gb_presets[validated_experiment_type]

    Int combine_archives_cpu = combine_archives_cpu_presets[validated_experiment_type]
    Float combine_archives_base_ram_per_file = combine_archives_base_ram_per_file_presets[validated_experiment_type]

    Int directDIA_single_vm_cpu = directDIA_single_vm_cpu_presets[
        validated_experiment_type]
    Int directDIA_single_vm_ram_gb = directDIA_single_vm_ram_gb_presets[
        validated_experiment_type]

    Int dia_analysis_cpu = dia_analysis_cpu_presets[validated_experiment_type]
    Int dia_analysis_ram_gb = dia_analysis_ram_gb_presets[validated_experiment_type]

    Int combine_sne_cpu = combine_sne_cpu_presets[validated_experiment_type]
    Float combine_sne_ram_per_file_merge = combine_sne_ram_per_file_merge_presets[validated_experiment_type]
    Float combine_sne_ram_per_file_combine = combine_sne_ram_per_file_combine_presets[validated_experiment_type]

    # ============================================================================
    # PHASE I: Discovery
    # ============================================================================

    # Validate do_pulsar + library combination before any expensive tasks
    call validate_skip_pulsar { input:
        skip_pulsar = !do_pulsar,
        has_library = has_library,
    }

    # List all raw files in the input directory and count them
    call list_files { input:
        gcs_path = file_directory,
    }

    # Calculate estimated size for disk allocation
    Float total_input_size_gb = list_files.num_files * average_file_size_gb

    # ============================================================================
    # PHASE II: HTRMS Conversion
    # ============================================================================
    # Flatten all bins to get individual files for conversion
    # Note: read_lines returns Array[String], which is correct since these are GCS paths
    Array[String] all_input_file_paths = read_lines(list_files.file_list)

    if (do_conversion) {
        # Scatter over each individual file (one file per VM)
        scatter (input_file_path in all_input_file_paths) {
            call htrms_conversion { input:
                input_file_path = input_file_path,
                convert_schema = convert_schema,
                n_preemptible = n_preemptible_htrms_conversion,
            }
        }

        call sum_floats { input:
            sizes = htrms_conversion.htrms_size_gb,
        }
    }

    # Select converted files or original files
    # htrms_conversion.htrms_file is Array[File]? result of scatter (Cromwell-managed files)
    # all_input_file_paths is Array[String] (GCS paths) — WDL development allows String→File coercion
    # Cromwell localizes Array[File] inputs to the task VM automatically
    Array[File] files_for_search = if defined(htrms_conversion.htrms_file) then select_first(
    [
        htrms_conversion.htrms_file,
    ]) else all_input_file_paths

    # ============================================================================
    # PHASE II (continued): Bin files for parallel processing
    # ============================================================================
    # Bins are created AFTER HTRMS conversion so that when do_conversion=true,
    # the bins contain the converted .htrms GCS paths (not the stale raw paths).
    # files_for_search already resolves to the correct paths in both cases.
    if (num_vms > 1) {
        call create_bins { input:
            file_paths = files_for_search,
            num_bins = num_vms,
        }
    }

    Float finalized_total_size_gb = select_first([
        sum_floats.total_size,
        total_input_size_gb,
    ])

    # Compute non-parallelized task RAM based on total file count (floor of 128 GB, capped at 750 GB)
    Int pulsar_step2_ram_gb = if (ceil(pulsar_step2_base_ram_per_file * list_files.num_files) > 750)
        then 750
        else if (ceil(pulsar_step2_base_ram_per_file * list_files.num_files) > 128)
            then ceil(pulsar_step2_base_ram_per_file * list_files.num_files)
            else 128

    Int combine_archives_ram_gb = if (ceil(combine_archives_base_ram_per_file * list_files.num_files) > 750)
        then 750
        else if (ceil(combine_archives_base_ram_per_file * list_files.num_files) > 128)
            then ceil(combine_archives_base_ram_per_file * list_files.num_files)
            else 128

    Float combine_sne_ram_per_file = if generate_sne_large_experiment then combine_sne_ram_per_file_merge else combine_sne_ram_per_file_combine
    Int combine_sne_ram_gb = if ceil(combine_sne_ram_per_file * list_files.num_files) > 700
        then 700
        else if ceil(combine_sne_ram_per_file * list_files.num_files) > 64
            then ceil(combine_sne_ram_per_file * list_files.num_files)
            else 64

    # ============================================================================
    # PHASE III: Conditional Execution - Single VM vs Parallel
    # ============================================================================

    # Determine if we need parallelization
    # If num_vms > 1, we proceed with binning. If num_vms == 1, we skip binning.

    # Logic:
    # If num_vms == 1: No binning. calculated_num_vms = 1.
    # If num_vms > 1: Run create_bins (already called above). calculated_num_vms comes from there.

    Int calculated_num_vms = if (num_vms > 1) then read_int(select_first([
        create_bins.calculated_num_vms_file,
    ])) else 1

    Array[Array[String]] search_file_bins = if (num_vms > 1) then read_json(select_first([
        create_bins.bins_json,
    ])) else []

    # BRANCH A: Single VM Mode (num_vms == 1)
    if (calculated_num_vms == 1) {
        # Run classic directDIA on all files in one VM
        call directDIA_single_vm { input:
            experiment_name = experiment_name,
            input_files = files_for_search,

            analysis_schema = directDIA_settings,
            fasta_1 = fasta_1,
            fasta_2 = fasta_2,
            fasta_3 = fasta_3,
            enzyme_database = enzyme_database,
            json_settings = json_settings,
            condition_setup = condition_setup,
            report_schema_1 = report_schema_1,
            report_schema_2 = report_schema_2,
            report_schema_3 = report_schema_3,
            report_schema_4 = report_schema_4,
            skip_pulsar = !do_pulsar,
            user_spectral_libraries = select_all([spectral_library_1, spectral_library_2, spectral_library_3]),
            cpu = directDIA_single_vm_cpu,
            ram_gb = directDIA_single_vm_ram_gb,
            n_preemptible = n_preemptible_directDIA_single_vm,
            allocated_disk_gb = ceil(finalized_total_size_gb * disk_size_multiplier),
            custom_mod_repository = custom_mod_repository,
        }
    }

    # BRANCH B: Parallel Mode (num_vms > 1)
    if (calculated_num_vms > 1) {

        # Calculate approximate size per VM for disk allocation
        Float bin_size_per_vm = finalized_total_size_gb / calculated_num_vms + 20

        # ========================================================================
        # STEPS 1.1-1.4: Pulsar pipeline (skipped when do_pulsar=false)
        # ========================================================================
        if (run_pulsar) {

            # ========================================================================
            # STEP 1.1: Generate intermediate search archives per bin (scatter)
            # ========================================================================
            scatter (i in range(length(search_file_bins))) {
                call pulsar_step1_binned { input:
                    input_files = search_file_bins[i],
                    analysis_schema = directDIA_settings,
                    fasta_1 = fasta_1,
                    fasta_2 = fasta_2,
                    fasta_3 = fasta_3,
                    enzyme_database = enzyme_database,
                    custom_mod_repository = custom_mod_repository,
                    cpu = pulsar_step1_cpu,
                    ram_gb = pulsar_step1_ram_gb,
                    bin_index = i,
                    n_preemptible = n_preemptible_pulsar_step1,
                    allocated_disk_gb = ceil(bin_size_per_vm * disk_size_multiplier),
                }
            }

            # Collect all intermediate archives (one per bin)
            Array[File] intermediate_archives = pulsar_step1_binned.intermediate_archive

            # ========================================================================
            # STEP 1.2: Combine intermediate archives to generate optimized models
            # ========================================================================
            call pulsar_step2_combine_models { input:
                intermediate_archives = intermediate_archives,
                experiment_name = experiment_name,

                cpu = pulsar_step2_cpu,
                ram_gb = pulsar_step2_ram_gb,
                enzyme_database = enzyme_database,
                custom_mod_repository = custom_mod_repository,
                analysis_schema = directDIA_settings,
                n_preemptible = n_preemptible_pulsar_step2,
                allocated_disk_gb = ceil(finalized_total_size_gb * disk_size_multiplier),
            }

            # ========================================================================
            # STEP 1.3: Generate final search archives per bin with optimized models
            # ========================================================================
            scatter (i in range(length(search_file_bins))) {
                call pulsar_step3_binned { input:
                    input_files = search_file_bins[i],
                    intermediate_archive = pulsar_step1_binned.intermediate_archive[i],
                    optimized_models = pulsar_step2_combine_models.optimized_models,
                    experiment_name = experiment_name,
                    analysis_schema = directDIA_settings,
                    enzyme_database = enzyme_database,
                    custom_mod_repository = custom_mod_repository,
                    cpu = pulsar_step3_cpu,
                    ram_gb = pulsar_step3_ram_gb,
                    bin_index = i,
                    n_preemptible = n_preemptible_pulsar_step3,
                    allocated_disk_gb = ceil(bin_size_per_vm * disk_size_multiplier),
                }
            }

            # Collect all final archives (one per bin)
            Array[File] final_archives = pulsar_step3_binned.final_archive

            # ========================================================================
            # STEP 1.4: Merge final search archives into single .kit library
            # ========================================================================
            call combine_final_archives { input:
                input_archives = final_archives,

                cpu = combine_archives_cpu,
                ram_gb = combine_archives_ram_gb,
                enzyme_database = enzyme_database,
                custom_mod_repository = custom_mod_repository,
                analysis_schema = directDIA_settings,
                n_preemptible = n_preemptible_combine_archives,
                allocated_disk_gb = ceil(finalized_total_size_gb * disk_size_multiplier),
            }

        }  # end if (run_pulsar)

        # Search archives are passed directly to dia_analysis_binned as two separate typed inputs:
        # - merged_archive: File? — Pulsar-generated .kit spectral library (combine_final_archives.combined_archive_library; undefined when do_pulsar=false)
        # - user_spectral_libraries: Array[String] — User-provided .kit GCS paths, downloaded at runtime
        #
        # Case: do_pulsar=true,  no libraries  → merged_archive defined, user_spectral_libraries empty
        # Case: do_pulsar=true,  ≥1 library    → merged_archive defined, user_spectral_libraries non-empty
        # Case: do_pulsar=false, ≥1 library    → merged_archive undefined, user_spectral_libraries non-empty
        # Case: do_pulsar=false, no libraries  → caught by validate_skip_pulsar before reaching here

        # ========================================================================
        # STEP 2: DIA analysis per bin against all search archives
        # ========================================================================
        scatter (i in range(length(search_file_bins))) {
            call dia_analysis_binned { input:
                experiment_name = experiment_name,
                input_files = search_file_bins[i],
                merged_archive = combine_final_archives.combined_archive_library,
                user_spectral_libraries = select_all([spectral_library_1, spectral_library_2, spectral_library_3]),
                analysis_schema = directDIA_settings,
                enzyme_database = enzyme_database,
                custom_mod_repository = custom_mod_repository,
                cpu = dia_analysis_cpu,
                ram_gb = dia_analysis_ram_gb,
                bin_index = i,
                n_preemptible = n_preemptible_dia_analysis,
                allocated_disk_gb = ceil(bin_size_per_vm * disk_size_multiplier),
            }
        }

        Array[File] all_sne = flatten(dia_analysis_binned.sne_files)

        # ========================================================================
        # STEP 3: Combine scattered SNE files and generate reports
        # ========================================================================
        call combine_sne { input:
            experiment_name = experiment_name,
            sne_files = all_sne,
            analysis_schema = directDIA_settings,
            condition_setup = condition_setup,
            report_schema_1 = report_schema_1,
            report_schema_2 = report_schema_2,
            report_schema_3 = report_schema_3,
            report_schema_4 = report_schema_4,
            cpu = combine_sne_cpu,
            ram_gb = combine_sne_ram_gb,
            enzyme_database = enzyme_database,
            custom_mod_repository = custom_mod_repository,
            n_preemptible = n_preemptible_combine_sne,
            allocated_disk_gb = ceil(finalized_total_size_gb * sne_combine_disk_size_multiplier),
            generate_sne_large_experiment = generate_sne_large_experiment,
        }
    }

    # Final output: select from single VM or parallel path
    output {
        File spectronaut_output = select_first([
            combine_sne.spectronaut_output,
            directDIA_single_vm.spectronaut_output,
        ])
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
        # Use || true to prevent grep from failing if no matches (set -e will exit otherwise)
        (grep -v 'TOTAL:' "${raw_listing}" || true) | \
            sed 's:/*$::' | \
            sed '/^[[:space:]]*$/d' | \
            (grep -v "^${normalized_path}$" || true) | \
            sort -u > "${cleaned_listing}"

        # Validate that at least one file was found
        file_count=$(wc -l < "${cleaned_listing}")
        if [ "${file_count}" -eq 0 ]; then
            echo "ERROR: No files found in directory: ~{gcs_path}" >&2
            exit 1
        fi
        echo "Found ${file_count} files in directory"

        # Write file count to output file
        echo "${file_count}" > num_files.txt

        # Ensure file is written to disk before Cromwell attempts delocalization
        sync
    >>>

    output {
        File file_list = "file_list.txt"
        Int num_files = read_int("num_files.txt")
    }

    runtime {
        docker: "gcr.io/google.com/cloudsdktool/cloud-sdk:stable"
        cpu: 4
        memory: "16GB"
        preemptible: 2
        bootDiskSizeGb: 20
        disks: "local-disk 100 HDD"
    }
}

task create_bins {
    input {
        Array[String] file_paths
        Int num_bins
    }

    command <<<
        python3 <<CODE
        import json
        import os

        cromwell_root = os.getcwd()

        # Read HTRMS file paths
        file_paths_file = "~{write_lines(file_paths)}"
        with open(file_paths_file) as f:
            files = [line.strip() for line in f if line.strip()]

        num_bins = ~{num_bins}

        # Cap num_bins at 80
        if num_bins > 80:
            print(f"WARNING: Requested {num_bins} VMs, capping at maximum of 80")
            num_bins = 80

        # Validate num_bins
        if num_bins < 1:
            print(f"WARNING: num_bins must be at least 1, got {num_bins}. Setting to 1.")
            num_bins = 1

        # Files pre-sorted by list_files task (line 290: sort -u)
        # Order preserved through scatter-gather (lines 103-113)
        print(f"Processing {len(files)} pre-sorted input files")

        # Calculate actual number of bins
        # Cannot have more bins than files
        actual_bins = min(num_bins, len(files))

        if actual_bins < num_bins:
            print(f"WARNING: Requested {num_bins} bins, but only {len(files)} files available")
            print(f"Setting actual_bins = {actual_bins}")

        # Ensure at least 1 bin if files exist
        if actual_bins < 1 and len(files) > 0:
            actual_bins = 1

        # Create bins using round-robin distribution
        bins = [[] for _ in range(actual_bins)]
        for i, file_path in enumerate(files):
            bin_index = i % actual_bins
            bins[bin_index].append(file_path)

        # Write bins to JSON
        with open(f"{cromwell_root}/bins.json", "w") as f:
            json.dump(bins, f, indent=2)

        # Write calculated_num_vms
        with open(f"{cromwell_root}/calculated_num_vms.txt", "w") as f:
            f.write(str(actual_bins))

        # Print summary
        print(f"Total files: {len(files)}")
        print(f"Calculated number of VMs: {actual_bins}")
        for i, bin_files in enumerate(bins):
            print(f"Bin {i}: {len(bin_files)} files")
        CODE
    >>>

    output {
        File bins_json = "bins.json"
        File calculated_num_vms_file = "calculated_num_vms.txt"
    }

    runtime {
        docker: "python:3.9-slim"
        cpu: 4
        memory: "16GB"
        preemptible: 2
        bootDiskSizeGb: 20
        disks: "local-disk 100 HDD"
    }
}

task htrms_conversion {
    input {
        String input_file_path
        Int n_preemptible
        File? convert_schema
    }

    # input_file_path is a GCS URI downloaded inside the command via gcloud storage cp.
    # Marking it localizationOptional prevents Cromwell from attempting to localize it as
    # a File, while still including its value in the call-caching hash key.
    parameter_meta {
        input_file_path: { localizationOptional: true }
    }

    command <<<
        set -euo pipefail

        cromwell_root=$(pwd)

        input_dir="${cromwell_root}/work_input"
        mkdir -p "${input_dir}"

        output_dir="${cromwell_root}/out_conversion"
        mkdir -p "${output_dir}"

        gcloud storage cp -r "~{input_file_path}" "${input_dir}/"

        # Run HTRMS conversion
        echo "Running HTRMS conversion..."
        spectronaut -convert \
            -i "${input_dir}" \
            -o "${output_dir}" \
            ~{if defined(convert_schema) then "-s " + convert_schema else ""} \
            2>&1 | tee htrms_conversion.log

        # Find and move the converted file(s)
        htrms_count=$(find "${output_dir}" -type f -name "*.htrms" | wc -l)
        if [ "${htrms_count}" -eq 0 ]; then
            echo "ERROR: No .htrms file produced" >&2
            exit 1
        fi
        echo "Moving ${htrms_count} HTRMS file(s)..."
        find "${output_dir}" -type f -name "*.htrms" -exec mv {} "${cromwell_root}/" \;
        echo "Conversion complete"
    >>>

    output {
        File htrms_file = glob("*.htrms")[0]
        Float htrms_size_gb = size(htrms_file, "GB")
    }

    runtime {
        docker: "broadcptacdev/panoply_spectronaut:v21.0"
        cpu: 16
        memory: "32GB"
        bootDiskSizeGb: 32
        disks: "local-disk 300 HDD"
        preemptible: n_preemptible
        # cpuPlatform: "AMD Milan"
    }
}

task directDIA_single_vm {
    input {
        File fasta_1
        File analysis_schema
        Array[String] input_files
        String experiment_name
        Int cpu
        Int ram_gb
        Int allocated_disk_gb
        Int n_preemptible
        File? fasta_2
        File? fasta_3
        File? enzyme_database
        File? json_settings
        File? condition_setup
        File? report_schema_1
        File? report_schema_2
        File? report_schema_3
        File? report_schema_4
        Boolean skip_pulsar
        Array[String] user_spectral_libraries
        File? custom_mod_repository
    }

    # input_files are GCS URIs (raw .d directories or converted .htrms files) and
    # user_spectral_libraries are GCS URIs (.kit files); both are downloaded inside the command
    # via gcloud storage cp -r. Marking them localizationOptional prevents Cromwell from
    # attempting to localize them as Files — which fails for timsTOF .d directories — while
    # still including their values in the call-caching hash key.
    parameter_meta {
        input_files: { localizationOptional: true }
        user_spectral_libraries: { localizationOptional: true }
    }

    command <<<
        set -euo pipefail

        cromwell_root=$(pwd)

        # Resource Monitoring Initialization
        wall_start=$(date +%s%N)  # Nanoseconds since epoch

        # Cgroup V2 (modern)
        if [ -f /sys/fs/cgroup/cpu.stat ]; then
            cpu_start=$(grep "usage_usec" /sys/fs/cgroup/cpu.stat | awk '{print $2}')
            cpu_start_ns=$((cpu_start * 1000))  # Convert microseconds to nanoseconds
        # Cgroup V1 (legacy)
        elif [ -f /sys/fs/cgroup/cpuacct/cpuacct.usage ]; then
            cpu_start_ns=$(cat /sys/fs/cgroup/cpuacct/cpuacct.usage)
        else
            cpu_start_ns=0
        fi

        # Capture per-CPU stats from /proc/stat
        cpu_stat_start=$(grep "^cpu[0-9]" /proc/stat 2>/dev/null || true)

        input_dir="${cromwell_root}/work_input"
        mkdir -p "${input_dir}"

        output_dir="${cromwell_root}/out_spectronaut"
        mkdir -p "${output_dir}"

        tmp_dir="${cromwell_root}/sn_temp"
        mkdir -p "${tmp_dir}"

        output_zip="${cromwell_root}/spectronaut_output.zip"

        # Download all input files from GCS to input directory.
        # gcloud storage cp -r handles both timsTOF .d directories and .htrms files.
        echo "Downloading input files to input directory..."
        while IFS= read -r input_file; do
            if [ -n "${input_file}" ]; then
                echo "Downloading: ${input_file}"
                gcloud storage cp -r "${input_file}" "${input_dir}/"
            fi
        done < ~{write_lines(input_files)}

        # Verify files were downloaded
        file_count=$(find "${input_dir}" -mindepth 1 -maxdepth 1 | wc -l)
        echo "Downloaded ${file_count} input item(s) to input directory"

        # Download user spectral libraries (if any) from GCS
        user_lib_dir="${cromwell_root}/user_libraries"
        mkdir -p "${user_lib_dir}"
        user_lib_args=""
        while IFS= read -r lib_path; do
            if [ -n "${lib_path}" ]; then
                echo "Downloading user spectral library: ${lib_path}"
                gcloud storage cp -r "${lib_path}" "${user_lib_dir}/"
                local_lib="${user_lib_dir}/$(basename "${lib_path}")"
                user_lib_args="${user_lib_args} -a ${local_lib}"
            fi
        done < ~{write_lines(user_spectral_libraries)}

        # Run spectronaut search
        if [ "~{skip_pulsar}" = "true" ]; then
            # do_pulsar=false: use diaanalysis command with user-provided spectral libraries
            echo "Starting DIA analysis (do_pulsar=false)..."
            spectronaut \
                -setTemp "${tmp_dir}" \
                ~{if defined(enzyme_database) then "--importEnzymeDB " + enzyme_database else ""} \
                ~{if defined(custom_mod_repository) then "--importModRepository " + custom_mod_repository else ""} \
                diaanalysis \
                -s "~{analysis_schema}" \
                ~{if defined(condition_setup) then "-con " + condition_setup else ""} \
                -fasta "~{fasta_1}" \
                ~{if defined(fasta_2) then "-fasta " + fasta_2 else ""} \
                ~{if defined(fasta_3) then "-fasta " + fasta_3 else ""} \
                ~{if defined(json_settings) then "-j " + json_settings else ""} \
                ~{if defined(report_schema_1) then "-rs " + report_schema_1 else ""} \
                ~{if defined(report_schema_2) then "-rs " + report_schema_2 else ""} \
                ~{if defined(report_schema_3) then "-rs " + report_schema_3 else ""} \
                ~{if defined(report_schema_4) then "-rs " + report_schema_4 else ""} \
                -n "~{experiment_name}" \
                -o "${output_dir}" \
                -d "${input_dir}" \
                ${user_lib_args} \
                2>&1 | tee spectronaut_single_vm.log
        else
            # do_pulsar=true: use direct (DirectDIA), optionally with user spectral libraries
            echo "Starting directDIA search..."
            spectronaut \
                -setTemp "${tmp_dir}" \
                ~{if defined(enzyme_database) then "--importEnzymeDB " + enzyme_database else ""} \
                ~{if defined(custom_mod_repository) then "--importModRepository " + custom_mod_repository else ""} \
                direct \
                -s "~{analysis_schema}" \
                ~{if defined(condition_setup) then "-con " + condition_setup else ""} \
                -fasta "~{fasta_1}" \
                ~{if defined(fasta_2) then "-fasta " + fasta_2 else ""} \
                ~{if defined(fasta_3) then "-fasta " + fasta_3 else ""} \
                ~{if defined(json_settings) then "-j " + json_settings else ""} \
                ~{if defined(report_schema_1) then "-rs " + report_schema_1 else ""} \
                ~{if defined(report_schema_2) then "-rs " + report_schema_2 else ""} \
                ~{if defined(report_schema_3) then "-rs " + report_schema_3 else ""} \
                ~{if defined(report_schema_4) then "-rs " + report_schema_4 else ""} \
                -n "~{experiment_name}" \
                -o "${output_dir}" \
                -d "${input_dir}" \
                ${user_lib_args} \
                2>&1 | tee spectronaut_single_vm.log
        fi

        # Create output archive
        echo "Creating output archive..."
        zip -r "${output_zip}" "${output_dir}" -x \*.zip

        if [ ! -f "${output_zip}" ]; then
            echo "ERROR: Failed to create output zip file" >&2
            exit 1
        fi

        echo "directDIA search complete."

        # ============================================================================
        # Resource Usage Report
        # ============================================================================
        echo "==========================================="
        echo "=== RESOURCE USAGE REPORT ==="
        echo "==========================================="

        # --- CPU Utilization ---
        echo ""
        echo "--- CPU Utilization ---"

        wall_end=$(date +%s%N)
        wall_elapsed_ns=$((wall_end - wall_start))

        # Cgroup V2
        if [ -f /sys/fs/cgroup/cpu.stat ]; then
            cpu_end=$(grep "usage_usec" /sys/fs/cgroup/cpu.stat | awk '{print $2}')
            cpu_end_ns=$((cpu_end * 1000))
        # Cgroup V1
        elif [ -f /sys/fs/cgroup/cpuacct/cpuacct.usage ]; then
            cpu_end_ns=$(cat /sys/fs/cgroup/cpuacct/cpuacct.usage)
        else
            cpu_end_ns=0
        fi

        cpu_stat_end=$(grep "^cpu[0-9]" /proc/stat 2>/dev/null || true)

        if [ "$cpu_start_ns" -gt 0 ] && [ "$cpu_end_ns" -gt "$cpu_start_ns" ] && [ "$wall_elapsed_ns" -gt 0 ]; then
            cpu_used_ns=$((cpu_end_ns - cpu_start_ns))
            cpu_utilization_total=$(awk -v cpu="$cpu_used_ns" -v wall="$wall_elapsed_ns" \
                'BEGIN { printf "%.2f", (cpu / wall) * 100 }')
            allocated_cpus=~{cpu}
            cpu_utilization_per_core=$(awk -v total="$cpu_utilization_total" -v cpus="$allocated_cpus" \
                'BEGIN { printf "%.2f", total / cpus }')

            echo "Allocated CPUs: ${allocated_cpus}"
            echo "Average CPU Utilization: ${cpu_utilization_total}% (across all cores)"

            if [ -n "$cpu_stat_start" ] && [ -n "$cpu_stat_end" ]; then
                max_core_util=$(paste \
                    <(echo "$cpu_stat_start" | awk '{idle=$5; total=0; for(i=2;i<=NF;i++) total+=$i; print idle, total}') \
                    <(echo "$cpu_stat_end"   | awk '{idle=$5; total=0; for(i=2;i<=NF;i++) total+=$i; print idle, total}') \
                    | awk '{
                        idle_start=$1; total_start=$2; idle_end=$3; total_end=$4;
                        d_total = total_end - total_start;
                        d_idle  = idle_end  - idle_start;
                        if (d_total > 0) util = (1 - d_idle/d_total) * 100; else util = 0;
                        if (util > max) max = util;
                      } END { printf "%.2f", max }')
                echo "Max Per-Core CPU Utilization: ${max_core_util}%"
            fi

            echo "Per-Core CPU Utilization: ${cpu_utilization_per_core}%"
            echo "Maximum Possible: $((allocated_cpus * 100))% (${allocated_cpus} cores at 100%)"
        else
            echo "CPU utilization data not available"
        fi

        # --- Memory Usage ---
        echo ""
        echo "--- Memory Usage ---"

        # Cgroup V2
        if [ -f /sys/fs/cgroup/memory.peak ]; then
            max_mem_bytes=$(cat /sys/fs/cgroup/memory.peak)
            limit_bytes=$(cat /sys/fs/cgroup/memory.max)
        # Cgroup V1
        elif [ -f /sys/fs/cgroup/memory/memory.max_usage_in_bytes ]; then
            max_mem_bytes=$(cat /sys/fs/cgroup/memory/memory.max_usage_in_bytes)
            limit_bytes=$(cat /sys/fs/cgroup/memory/memory.limit_in_bytes)
        else
            max_mem_bytes=0
            limit_bytes=0
        fi

        if [ "$max_mem_bytes" -gt 0 ]; then
            max_mem_gb=$(awk -v val="$max_mem_bytes" 'BEGIN { printf "%.2f", val / (1024^3) }')
            echo "Actual Peak RAM: ${max_mem_gb} GB"

            if [ "$limit_bytes" != "max" ] && [ "$limit_bytes" -gt 0 ]; then
                limit_gb=$(awk -v val="$limit_bytes" 'BEGIN { printf "%.2f", val / (1024^3) }')
                usage_percent=$(awk -v max="$max_mem_bytes" -v lim="$limit_bytes" \
                    'BEGIN { printf "%.2f", (max / lim) * 100 }')
                echo "RAM Limit: ${limit_gb} GB"
                echo "RAM Usage: ${usage_percent}%"
            fi
        else
            echo "Memory usage data not available"
        fi

        # --- Disk Usage ---
        echo ""
        echo "--- Disk Usage ---"

        allocated_disk_gb=~{allocated_disk_gb}
        echo "Disk Size Assigned: ${allocated_disk_gb} GB"

        if command -v df >/dev/null 2>&1; then
            disk_used_output=$(df -BG "${cromwell_root}" 2>/dev/null | tail -1 | awk '{print $3}')

            if [ -n "${disk_used_output}" ]; then
                disk_used_gb=$(echo "${disk_used_output}" | sed 's/G$//')
                disk_usage_percent=$(awk -v used="$disk_used_gb" -v alloc="$allocated_disk_gb" \
                    'BEGIN { printf "%.2f", (used / alloc) * 100 }')

                echo "Final Disk Used: ${disk_used_gb} GB"
                echo "Disk Usage: ${disk_usage_percent}%"
            else
                echo "Could not measure disk usage"
            fi
        else
            echo "df command not available"
        fi

        echo ""
        echo "==========================================="
    >>>

    output {
        File spectronaut_output = "spectronaut_output.zip"
    }

    runtime {
        docker: "broadcptacdev/panoply_spectronaut:v21.0"
        cpu: cpu
        memory: "~{ram_gb}GB"
        bootDiskSizeGb: 32
        disks: "local-disk ~{allocated_disk_gb} HDD"
        preemptible: n_preemptible
        # cpuPlatform: "AMD Milan"
    }
}

task pulsar_step1_binned {
    input {
        File fasta_1
        File analysis_schema
        Array[String] input_files
        Int cpu
        Int ram_gb
        Int bin_index
        Int allocated_disk_gb
        Int n_preemptible
        File? fasta_2
        File? fasta_3
        File? enzyme_database
        File? custom_mod_repository
    }

    # input_files are GCS URIs (raw .d directories or converted .htrms files) downloaded
    # inside the command via gcloud storage cp -r. Marking them localizationOptional prevents
    # Cromwell from attempting to localize them as Files — which fails for timsTOF .d
    # directories — while still including their values in the call-caching hash key.
    parameter_meta {
        input_files: { localizationOptional: true }
    }

    command <<<
        set -euo pipefail

        cromwell_root=$(pwd)

        # Resource Monitoring Initialization
        wall_start=$(date +%s%N)  # Nanoseconds since epoch

        # Cgroup V2 (modern)
        if [ -f /sys/fs/cgroup/cpu.stat ]; then
            cpu_start=$(grep "usage_usec" /sys/fs/cgroup/cpu.stat | awk '{print $2}')
            cpu_start_ns=$((cpu_start * 1000))  # Convert microseconds to nanoseconds
        # Cgroup V1 (legacy)
        elif [ -f /sys/fs/cgroup/cpuacct/cpuacct.usage ]; then
            cpu_start_ns=$(cat /sys/fs/cgroup/cpuacct/cpuacct.usage)
        else
            cpu_start_ns=0
        fi

        # Capture per-CPU stats from /proc/stat
        cpu_stat_start=$(grep "^cpu[0-9]" /proc/stat 2>/dev/null || true)

        input_dir="${cromwell_root}/work_input"
        mkdir -p "${input_dir}"

        output_dir="${cromwell_root}/out_pulsar_step1"
        mkdir -p "${output_dir}"

        tmp_dir="${cromwell_root}/sn_temp"
        mkdir -p "${tmp_dir}"

        # Download ALL binned files from GCS to input directory.
        # gcloud storage cp -r handles both timsTOF .d directories and .htrms files.
        echo "Downloading all input files for bin ~{bin_index}..."
        while IFS= read -r input_file; do
            if [ -n "${input_file}" ]; then
                echo "Downloading: ${input_file}"
                gcloud storage cp -r "${input_file}" "${input_dir}/"
            fi
        done < ~{write_lines(input_files)}

        # Verify files were downloaded
        file_count=$(find "${input_dir}" -mindepth 1 -maxdepth 1 | wc -l)
        echo "Downloaded ${file_count} input item(s) to input directory for bin ~{bin_index}"

        # Run Pulsar Step 1 on ALL files in the bin at once (batch processing)
        echo "Running Pulsar Step 1 for bin ~{bin_index} (batch processing ${file_count} files)..."

        # List files for troubleshooting
        echo "Files in input directory:"
        ls -1 "${input_dir}"

        # Construct input flags based on actual file type:
        # HTRMS files require -r per file; raw files (incl. timsTOF .d directories) use -d directory.
        # Detect HTRMS by top-level .htrms entries so that files inside a .d directory are ignored.
        htrms_count=$(find "${input_dir}" -mindepth 1 -maxdepth 1 -name "*.htrms" | wc -l)
        if [ "${htrms_count}" -gt 0 ]; then
            cmd_flags=""
            for f in "${input_dir}"/*.htrms; do
                 if [ -f "$f" ]; then
                    cmd_flags="${cmd_flags} -r $f"
                 fi
            done
        else
            cmd_flags="-d ${input_dir}"
        fi

        # Prepare command string for printing (back-tracing)
        cmd_string="spectronaut -setTemp ${tmp_dir} ~{if defined(enzyme_database) then "--importEnzymeDB " + enzyme_database else ""} ~{if defined(custom_mod_repository) then "--importModRepository " + custom_mod_repository else ""} lg -se Pulsar ${cmd_flags} -fasta ~{fasta_1} ~{if defined(
             fasta_2) then "-fasta " + fasta_2 else ""} ~{if defined(fasta_3) then "-fasta "
             + fasta_3 else ""} -rs ~{analysis_schema} --pulsarStage pulsarStep1 -a ${output_dir}/search_archive_step1_bin_~{
             bin_index}.psar -o ${output_dir}"

        echo "Command being run:"
        echo "${cmd_string}"

        spectronaut \
            -setTemp "${tmp_dir}" \
            ~{if defined(enzyme_database) then "--importEnzymeDB " + enzyme_database else ""} \
            ~{if defined(custom_mod_repository) then "--importModRepository " + custom_mod_repository else ""} \
            lg -se Pulsar \
            ${cmd_flags} \
            -fasta "~{fasta_1}" \
            ~{if defined(fasta_2) then "-fasta " + fasta_2 else ""} \
            ~{if defined(fasta_3) then "-fasta " + fasta_3 else ""} \
            -rs "~{analysis_schema}" \
            --pulsarStage pulsarStep1 \
            -a "${output_dir}/search_archive_step1_bin_~{bin_index}.psar" \
            -o "${output_dir}" \
            2>&1 | tee pulsar_step1_bin_~{bin_index}.log

        # Verify output and move to cromwell root
        psar_count=$(find "${output_dir}" -type f -name "*.psar" | wc -l)
        if [ "${psar_count}" -eq 0 ]; then
            echo "ERROR: No .psar file produced for bin ~{bin_index}" >&2
            echo "Output directory contents:" >&2
            ls -lh "${output_dir}" >&2
            exit 1
        fi
        echo "Moving ${psar_count} PSAR file(s)..."
        find "${output_dir}" -type f -name "*.psar" -exec mv {} "${cromwell_root}/" \;
        echo "Generated intermediate search archive for bin ~{bin_index}"

        # ============================================================================
        # Resource Usage Report
        # ============================================================================
        echo "==========================================="
        echo "=== RESOURCE USAGE REPORT ==="
        echo "==========================================="

        # --- CPU Utilization ---
        echo ""
        echo "--- CPU Utilization ---"

        wall_end=$(date +%s%N)
        wall_elapsed_ns=$((wall_end - wall_start))

        # Cgroup V2
        if [ -f /sys/fs/cgroup/cpu.stat ]; then
            cpu_end=$(grep "usage_usec" /sys/fs/cgroup/cpu.stat | awk '{print $2}')
            cpu_end_ns=$((cpu_end * 1000))
        # Cgroup V1
        elif [ -f /sys/fs/cgroup/cpuacct/cpuacct.usage ]; then
            cpu_end_ns=$(cat /sys/fs/cgroup/cpuacct/cpuacct.usage)
        else
            cpu_end_ns=0
        fi

        cpu_stat_end=$(grep "^cpu[0-9]" /proc/stat 2>/dev/null || true)

        if [ "$cpu_start_ns" -gt 0 ] && [ "$cpu_end_ns" -gt "$cpu_start_ns" ] && [ "$wall_elapsed_ns" -gt 0 ]; then
            cpu_used_ns=$((cpu_end_ns - cpu_start_ns))
            cpu_utilization_total=$(awk -v cpu="$cpu_used_ns" -v wall="$wall_elapsed_ns" \
                'BEGIN { printf "%.2f", (cpu / wall) * 100 }')
            allocated_cpus=~{cpu}
            cpu_utilization_per_core=$(awk -v total="$cpu_utilization_total" -v cpus="$allocated_cpus" \
                'BEGIN { printf "%.2f", total / cpus }')

            echo "Allocated CPUs: ${allocated_cpus}"
            echo "Average CPU Utilization: ${cpu_utilization_total}% (across all cores)"

            if [ -n "$cpu_stat_start" ] && [ -n "$cpu_stat_end" ]; then
                max_core_util=$(paste \
                    <(echo "$cpu_stat_start" | awk '{idle=$5; total=0; for(i=2;i<=NF;i++) total+=$i; print idle, total}') \
                    <(echo "$cpu_stat_end"   | awk '{idle=$5; total=0; for(i=2;i<=NF;i++) total+=$i; print idle, total}') \
                    | awk '{
                        idle_start=$1; total_start=$2; idle_end=$3; total_end=$4;
                        d_total = total_end - total_start;
                        d_idle  = idle_end  - idle_start;
                        if (d_total > 0) util = (1 - d_idle/d_total) * 100; else util = 0;
                        if (util > max) max = util;
                      } END { printf "%.2f", max }')
                echo "Max Per-Core CPU Utilization: ${max_core_util}%"
            fi

            echo "Per-Core CPU Utilization: ${cpu_utilization_per_core}%"
            echo "Maximum Possible: $((allocated_cpus * 100))% (${allocated_cpus} cores at 100%)"
        else
            echo "CPU utilization data not available"
        fi

        # --- Memory Usage ---
        echo ""
        echo "--- Memory Usage ---"

        # Cgroup V2
        if [ -f /sys/fs/cgroup/memory.peak ]; then
            max_mem_bytes=$(cat /sys/fs/cgroup/memory.peak)
            limit_bytes=$(cat /sys/fs/cgroup/memory.max)
        # Cgroup V1
        elif [ -f /sys/fs/cgroup/memory/memory.max_usage_in_bytes ]; then
            max_mem_bytes=$(cat /sys/fs/cgroup/memory/memory.max_usage_in_bytes)
            limit_bytes=$(cat /sys/fs/cgroup/memory/memory.limit_in_bytes)
        else
            max_mem_bytes=0
            limit_bytes=0
        fi

        if [ "$max_mem_bytes" -gt 0 ]; then
            max_mem_gb=$(awk -v val="$max_mem_bytes" 'BEGIN { printf "%.2f", val / (1024^3) }')
            echo "Actual Peak RAM: ${max_mem_gb} GB"

            if [ "$limit_bytes" != "max" ] && [ "$limit_bytes" -gt 0 ]; then
                limit_gb=$(awk -v val="$limit_bytes" 'BEGIN { printf "%.2f", val / (1024^3) }')
                usage_percent=$(awk -v max="$max_mem_bytes" -v lim="$limit_bytes" \
                    'BEGIN { printf "%.2f", (max / lim) * 100 }')
                echo "RAM Limit: ${limit_gb} GB"
                echo "RAM Usage: ${usage_percent}%"
            fi
        else
            echo "Memory usage data not available"
        fi

        # --- Disk Usage ---
        echo ""
        echo "--- Disk Usage ---"

        allocated_disk_gb=~{allocated_disk_gb}
        echo "Disk Size Assigned: ${allocated_disk_gb} GB"

        if command -v df >/dev/null 2>&1; then
            disk_used_output=$(df -BG "${cromwell_root}" 2>/dev/null | tail -1 | awk '{print $3}')

            if [ -n "${disk_used_output}" ]; then
                disk_used_gb=$(echo "${disk_used_output}" | sed 's/G$//')
                disk_usage_percent=$(awk -v used="$disk_used_gb" -v alloc="$allocated_disk_gb" \
                    'BEGIN { printf "%.2f", (used / alloc) * 100 }')

                echo "Final Disk Used: ${disk_used_gb} GB"
                echo "Disk Usage: ${disk_usage_percent}%"
            else
                echo "Could not measure disk usage"
            fi
        else
            echo "df command not available"
        fi

        echo ""
        echo "==========================================="
    >>>

    output {
        File intermediate_archive = glob("*.psar")[0]
    }

    runtime {
        docker: "broadcptacdev/panoply_spectronaut:v21.0"
        cpu: cpu
        memory: "~{ram_gb}GB"
        bootDiskSizeGb: 32
        disks: "local-disk ~{allocated_disk_gb} HDD"
        preemptible: n_preemptible
        # cpuPlatform: "AMD Milan"
    }
}

task pulsar_step2_combine_models {
    input {
        File analysis_schema
        Array[File] intermediate_archives
        String experiment_name
        Int cpu
        Int ram_gb
        Int allocated_disk_gb
        Int n_preemptible
        File? enzyme_database
        File? custom_mod_repository
    }

    command <<<
        set -euo pipefail

        cromwell_root=$(pwd)

        # Resource Monitoring Initialization
        wall_start=$(date +%s%N)  # Nanoseconds since epoch

        # Cgroup V2 (modern)
        if [ -f /sys/fs/cgroup/cpu.stat ]; then
            cpu_start=$(grep "usage_usec" /sys/fs/cgroup/cpu.stat | awk '{print $2}')
            cpu_start_ns=$((cpu_start * 1000))  # Convert microseconds to nanoseconds
        # Cgroup V1 (legacy)
        elif [ -f /sys/fs/cgroup/cpuacct/cpuacct.usage ]; then
            cpu_start_ns=$(cat /sys/fs/cgroup/cpuacct/cpuacct.usage)
        else
            cpu_start_ns=0
        fi

        # Capture per-CPU stats from /proc/stat
        cpu_stat_start=$(grep "^cpu[0-9]" /proc/stat 2>/dev/null || true)

        archives_dir="${cromwell_root}/work_archives_step1"
        output_dir="${cromwell_root}/out_pulsar_step2"
        tmp_dir="${cromwell_root}/sn_temp"

        mkdir -p "${archives_dir}" "${output_dir}" "${tmp_dir}"

        # Copy all intermediate .psar files from all bins
        echo "Copying intermediate archives from all bins..."
        while IFS= read -r archive; do
            if [ -n "${archive}" ]; then
                cp "${archive}" "${archives_dir}/"
            fi
        done < ~{write_lines(intermediate_archives)}

        # Verify archives were copied
        archive_count=$(find "${archives_dir}" -type f -name "*.psar" | wc -l)
        echo "Copied ${archive_count} intermediate archives"

        # Run Pulsar Step 2 to generate .qsp optimized models
        echo "Running Pulsar Step 2 to generate optimized models from ${archive_count} archives..."
        spectronaut \
            -setTemp "${tmp_dir}" \
            ~{if defined(enzyme_database) then "--importEnzymeDB " + enzyme_database else ""} \
            ~{if defined(custom_mod_repository) then "--importModRepository " + custom_mod_repository else ""} \
            lg -se Pulsar \
            -sad "${archives_dir}" \
            --pulsarStage pulsarStep2 \
            -n "~{experiment_name}" \
            -rs "~{analysis_schema}" \
            --noOutputSubfolder \
            -o "${output_dir}" \
            2>&1 | tee pulsar_step2_combine.log

        # Verify output and move to cromwell root
        qsp_count=$(find "${output_dir}" -type f -name "*.qsp" | wc -l)
        if [ "${qsp_count}" -eq 0 ]; then
            echo "ERROR: No .qsp optimized models file produced" >&2
            echo "Output directory contents:" >&2
            ls -lh "${output_dir}" >&2
            exit 1
        fi
        echo "Moving ${qsp_count} QSP file(s)..."
        find "${output_dir}" -type f -name "*.qsp" -exec mv {} "${cromwell_root}/" \;
        echo "Generated optimized models"

        # ============================================================================
        # Resource Usage Report
        # ============================================================================
        echo "==========================================="
        echo "=== RESOURCE USAGE REPORT ==="
        echo "==========================================="

        # --- CPU Utilization ---
        echo ""
        echo "--- CPU Utilization ---"

        wall_end=$(date +%s%N)
        wall_elapsed_ns=$((wall_end - wall_start))

        # Cgroup V2
        if [ -f /sys/fs/cgroup/cpu.stat ]; then
            cpu_end=$(grep "usage_usec" /sys/fs/cgroup/cpu.stat | awk '{print $2}')
            cpu_end_ns=$((cpu_end * 1000))
        # Cgroup V1
        elif [ -f /sys/fs/cgroup/cpuacct/cpuacct.usage ]; then
            cpu_end_ns=$(cat /sys/fs/cgroup/cpuacct/cpuacct.usage)
        else
            cpu_end_ns=0
        fi

        cpu_stat_end=$(grep "^cpu[0-9]" /proc/stat 2>/dev/null || true)

        if [ "$cpu_start_ns" -gt 0 ] && [ "$cpu_end_ns" -gt "$cpu_start_ns" ] && [ "$wall_elapsed_ns" -gt 0 ]; then
            cpu_used_ns=$((cpu_end_ns - cpu_start_ns))
            cpu_utilization_total=$(awk -v cpu="$cpu_used_ns" -v wall="$wall_elapsed_ns" \
                'BEGIN { printf "%.2f", (cpu / wall) * 100 }')
            allocated_cpus=~{cpu}
            cpu_utilization_per_core=$(awk -v total="$cpu_utilization_total" -v cpus="$allocated_cpus" \
                'BEGIN { printf "%.2f", total / cpus }')

            echo "Allocated CPUs: ${allocated_cpus}"
            echo "Average CPU Utilization: ${cpu_utilization_total}% (across all cores)"

            if [ -n "$cpu_stat_start" ] && [ -n "$cpu_stat_end" ]; then
                max_core_util=$(paste \
                    <(echo "$cpu_stat_start" | awk '{idle=$5; total=0; for(i=2;i<=NF;i++) total+=$i; print idle, total}') \
                    <(echo "$cpu_stat_end"   | awk '{idle=$5; total=0; for(i=2;i<=NF;i++) total+=$i; print idle, total}') \
                    | awk '{
                        idle_start=$1; total_start=$2; idle_end=$3; total_end=$4;
                        d_total = total_end - total_start;
                        d_idle  = idle_end  - idle_start;
                        if (d_total > 0) util = (1 - d_idle/d_total) * 100; else util = 0;
                        if (util > max) max = util;
                      } END { printf "%.2f", max }')
                echo "Max Per-Core CPU Utilization: ${max_core_util}%"
            fi

            echo "Per-Core CPU Utilization: ${cpu_utilization_per_core}%"
            echo "Maximum Possible: $((allocated_cpus * 100))% (${allocated_cpus} cores at 100%)"
        else
            echo "CPU utilization data not available"
        fi

        # --- Memory Usage ---
        echo ""
        echo "--- Memory Usage ---"

        # Cgroup V2
        if [ -f /sys/fs/cgroup/memory.peak ]; then
            max_mem_bytes=$(cat /sys/fs/cgroup/memory.peak)
            limit_bytes=$(cat /sys/fs/cgroup/memory.max)
        # Cgroup V1
        elif [ -f /sys/fs/cgroup/memory/memory.max_usage_in_bytes ]; then
            max_mem_bytes=$(cat /sys/fs/cgroup/memory/memory.max_usage_in_bytes)
            limit_bytes=$(cat /sys/fs/cgroup/memory/memory.limit_in_bytes)
        else
            max_mem_bytes=0
            limit_bytes=0
        fi

        if [ "$max_mem_bytes" -gt 0 ]; then
            max_mem_gb=$(awk -v val="$max_mem_bytes" 'BEGIN { printf "%.2f", val / (1024^3) }')
            echo "Actual Peak RAM: ${max_mem_gb} GB"

            if [ "$limit_bytes" != "max" ] && [ "$limit_bytes" -gt 0 ]; then
                limit_gb=$(awk -v val="$limit_bytes" 'BEGIN { printf "%.2f", val / (1024^3) }')
                usage_percent=$(awk -v max="$max_mem_bytes" -v lim="$limit_bytes" \
                    'BEGIN { printf "%.2f", (max / lim) * 100 }')
                echo "RAM Limit: ${limit_gb} GB"
                echo "RAM Usage: ${usage_percent}%"
            fi
        else
            echo "Memory usage data not available"
        fi

        # --- Disk Usage ---
        echo ""
        echo "--- Disk Usage ---"

        allocated_disk_gb=~{allocated_disk_gb}
        echo "Disk Size Assigned: ${allocated_disk_gb} GB"

        if command -v df >/dev/null 2>&1; then
            disk_used_output=$(df -BG "${cromwell_root}" 2>/dev/null | tail -1 | awk '{print $3}')

            if [ -n "${disk_used_output}" ]; then
                disk_used_gb=$(echo "${disk_used_output}" | sed 's/G$//')
                disk_usage_percent=$(awk -v used="$disk_used_gb" -v alloc="$allocated_disk_gb" \
                    'BEGIN { printf "%.2f", (used / alloc) * 100 }')

                echo "Final Disk Used: ${disk_used_gb} GB"
                echo "Disk Usage: ${disk_usage_percent}%"
            else
                echo "Could not measure disk usage"
            fi
        else
            echo "df command not available"
        fi

        echo ""
        echo "==========================================="
    >>>

    output {
        File optimized_models = glob("*.qsp")[0]
    }

    runtime {
        docker: "broadcptacdev/panoply_spectronaut:v21.0"
        cpu: cpu
        memory: "~{ram_gb}GB"
        bootDiskSizeGb: 32
        disks: "local-disk ~{allocated_disk_gb} HDD"
        preemptible: n_preemptible
        # cpuPlatform: "AMD Milan"
    }
}

task pulsar_step3_binned {
    input {
        File intermediate_archive
        File optimized_models
        File analysis_schema
        Array[String] input_files
        String experiment_name
        Int cpu
        Int ram_gb
        Int bin_index
        Int allocated_disk_gb
        Int n_preemptible
        File? enzyme_database
        File? custom_mod_repository
    }

    # input_files are GCS URIs (raw .d directories or converted .htrms files) downloaded
    # inside the command via gcloud storage cp -r. Marking them localizationOptional prevents
    # Cromwell from attempting to localize them as Files — which fails for timsTOF .d
    # directories — while still including their values in the call-caching hash key.
    parameter_meta {
        input_files: { localizationOptional: true }
    }

    command <<<
        set -euo pipefail

        cromwell_root=$(pwd)

        # Resource Monitoring Initialization
        wall_start=$(date +%s%N)  # Nanoseconds since epoch

        # Cgroup V2 (modern)
        if [ -f /sys/fs/cgroup/cpu.stat ]; then
            cpu_start=$(grep "usage_usec" /sys/fs/cgroup/cpu.stat | awk '{print $2}')
            cpu_start_ns=$((cpu_start * 1000))  # Convert microseconds to nanoseconds
        # Cgroup V1 (legacy)
        elif [ -f /sys/fs/cgroup/cpuacct/cpuacct.usage ]; then
            cpu_start_ns=$(cat /sys/fs/cgroup/cpuacct/cpuacct.usage)
        else
            cpu_start_ns=0
        fi

        # Capture per-CPU stats from /proc/stat
        cpu_stat_start=$(grep "^cpu[0-9]" /proc/stat 2>/dev/null || true)

        input_dir="${cromwell_root}/work_input"
        output_dir="${cromwell_root}/out_pulsar_step3"
        tmp_dir="${cromwell_root}/sn_temp"

        mkdir -p "${input_dir}" "${output_dir}" "${tmp_dir}"

        # Download ALL binned files from GCS to input_dir.
        # gcloud storage cp -r handles both timsTOF .d directories and .htrms files.
        echo "Downloading input files for bin ~{bin_index}..."
        while IFS= read -r input_file; do
            if [ -n "${input_file}" ]; then
                echo "Downloading: ${input_file}"
                gcloud storage cp -r "${input_file}" "${input_dir}/"
            fi
        done < ~{write_lines(input_files)}

        # Verify files were downloaded
        file_count=$(find "${input_dir}" -mindepth 1 -maxdepth 1 | wc -l)
        echo "Downloaded ${file_count} input item(s) to input directory for bin ~{bin_index}"

        # Run Pulsar Step 3 with optimized models (batch processing)
        echo "Running Pulsar Step 3 for bin ~{bin_index} with optimized models..."

        # List files for troubleshooting
        echo "Files in input directory:"
        ls -1 "${input_dir}"

        # Construct input flags based on actual file type:
        # HTRMS files require -r per file; raw files (incl. timsTOF .d directories) use -d directory.
        # Detect HTRMS by top-level .htrms entries so that files inside a .d directory are ignored.
        htrms_count=$(find "${input_dir}" -mindepth 1 -maxdepth 1 -name "*.htrms" | wc -l)
        if [ "${htrms_count}" -gt 0 ]; then
            cmd_flags=""
            for f in "${input_dir}"/*.htrms; do
                 if [ -f "$f" ]; then
                    cmd_flags="${cmd_flags} -r $f"
                 fi
            done
        else
            cmd_flags="-d ${input_dir}"
        fi

        # Prepare command string for printing (back-tracing)
        cmd_string="spectronaut -setTemp ${tmp_dir} ~{if defined(enzyme_database) then "--importEnzymeDB " + enzyme_database else ""} ~{if defined(custom_mod_repository) then "--importModRepository " + custom_mod_repository else ""} lg -se Pulsar ${cmd_flags} -sa ~{intermediate_archive} -a ${output_dir}/search_archive_bin_~{
             bin_index}.psar --optimizedModels ~{optimized_models} --pulsarStage pulsarStep3 -o ${output_dir} -n ~{
             experiment_name}_bin_~{bin_index} -rs ~{analysis_schema}"

        echo "Command being run:"
        echo "${cmd_string}"

        spectronaut \
            -setTemp "${tmp_dir}" \
            ~{if defined(enzyme_database) then "--importEnzymeDB " + enzyme_database else ""} \
            ~{if defined(custom_mod_repository) then "--importModRepository " + custom_mod_repository else ""} \
            lg -se Pulsar \
            ${cmd_flags} \
            -sa "~{intermediate_archive}" \
            -a "${output_dir}/search_archive_bin_~{bin_index}.psar" \
            --optimizedModels "~{optimized_models}" \
            --pulsarStage pulsarStep3 \
            -o "${output_dir}" \
            -n "~{experiment_name}_bin_~{bin_index}" \
            -rs "~{analysis_schema}" \
            2>&1 | tee pulsar_step3_bin_~{bin_index}.log

        # Verify output and move to cromwell root
        psar_count=$(find "${output_dir}" -type f -name "*.psar" | wc -l)
        if [ "${psar_count}" -eq 0 ]; then
            echo "ERROR: No .psar file produced for bin ~{bin_index}" >&2
            echo "Output directory contents:" >&2
            ls -lh "${output_dir}" >&2
            exit 1
        fi
        echo "Moving ${psar_count} PSAR file(s)..."
        find "${output_dir}" -type f -name "*.psar" -exec mv {} "${cromwell_root}/" \;
        echo "Generated final search archive for bin ~{bin_index}"

        # ============================================================================
        # Resource Usage Report
        # ============================================================================
        echo "==========================================="
        echo "=== RESOURCE USAGE REPORT ==="
        echo "==========================================="

        # --- CPU Utilization ---
        echo ""
        echo "--- CPU Utilization ---"

        wall_end=$(date +%s%N)
        wall_elapsed_ns=$((wall_end - wall_start))

        # Cgroup V2
        if [ -f /sys/fs/cgroup/cpu.stat ]; then
            cpu_end=$(grep "usage_usec" /sys/fs/cgroup/cpu.stat | awk '{print $2}')
            cpu_end_ns=$((cpu_end * 1000))
        # Cgroup V1
        elif [ -f /sys/fs/cgroup/cpuacct/cpuacct.usage ]; then
            cpu_end_ns=$(cat /sys/fs/cgroup/cpuacct/cpuacct.usage)
        else
            cpu_end_ns=0
        fi

        cpu_stat_end=$(grep "^cpu[0-9]" /proc/stat 2>/dev/null || true)

        if [ "$cpu_start_ns" -gt 0 ] && [ "$cpu_end_ns" -gt "$cpu_start_ns" ] && [ "$wall_elapsed_ns" -gt 0 ]; then
            cpu_used_ns=$((cpu_end_ns - cpu_start_ns))
            cpu_utilization_total=$(awk -v cpu="$cpu_used_ns" -v wall="$wall_elapsed_ns" \
                'BEGIN { printf "%.2f", (cpu / wall) * 100 }')
            allocated_cpus=~{cpu}
            cpu_utilization_per_core=$(awk -v total="$cpu_utilization_total" -v cpus="$allocated_cpus" \
                'BEGIN { printf "%.2f", total / cpus }')

            echo "Allocated CPUs: ${allocated_cpus}"
            echo "Average CPU Utilization: ${cpu_utilization_total}% (across all cores)"

            if [ -n "$cpu_stat_start" ] && [ -n "$cpu_stat_end" ]; then
                max_core_util=$(paste \
                    <(echo "$cpu_stat_start" | awk '{idle=$5; total=0; for(i=2;i<=NF;i++) total+=$i; print idle, total}') \
                    <(echo "$cpu_stat_end"   | awk '{idle=$5; total=0; for(i=2;i<=NF;i++) total+=$i; print idle, total}') \
                    | awk '{
                        idle_start=$1; total_start=$2; idle_end=$3; total_end=$4;
                        d_total = total_end - total_start;
                        d_idle  = idle_end  - idle_start;
                        if (d_total > 0) util = (1 - d_idle/d_total) * 100; else util = 0;
                        if (util > max) max = util;
                      } END { printf "%.2f", max }')
                echo "Max Per-Core CPU Utilization: ${max_core_util}%"
            fi

            echo "Per-Core CPU Utilization: ${cpu_utilization_per_core}%"
            echo "Maximum Possible: $((allocated_cpus * 100))% (${allocated_cpus} cores at 100%)"
        else
            echo "CPU utilization data not available"
        fi

        # --- Memory Usage ---
        echo ""
        echo "--- Memory Usage ---"

        # Cgroup V2
        if [ -f /sys/fs/cgroup/memory.peak ]; then
            max_mem_bytes=$(cat /sys/fs/cgroup/memory.peak)
            limit_bytes=$(cat /sys/fs/cgroup/memory.max)
        # Cgroup V1
        elif [ -f /sys/fs/cgroup/memory/memory.max_usage_in_bytes ]; then
            max_mem_bytes=$(cat /sys/fs/cgroup/memory/memory.max_usage_in_bytes)
            limit_bytes=$(cat /sys/fs/cgroup/memory/memory.limit_in_bytes)
        else
            max_mem_bytes=0
            limit_bytes=0
        fi

        if [ "$max_mem_bytes" -gt 0 ]; then
            max_mem_gb=$(awk -v val="$max_mem_bytes" 'BEGIN { printf "%.2f", val / (1024^3) }')
            echo "Actual Peak RAM: ${max_mem_gb} GB"

            if [ "$limit_bytes" != "max" ] && [ "$limit_bytes" -gt 0 ]; then
                limit_gb=$(awk -v val="$limit_bytes" 'BEGIN { printf "%.2f", val / (1024^3) }')
                usage_percent=$(awk -v max="$max_mem_bytes" -v lim="$limit_bytes" \
                    'BEGIN { printf "%.2f", (max / lim) * 100 }')
                echo "RAM Limit: ${limit_gb} GB"
                echo "RAM Usage: ${usage_percent}%"
            fi
        else
            echo "Memory usage data not available"
        fi

        # --- Disk Usage ---
        echo ""
        echo "--- Disk Usage ---"

        allocated_disk_gb=~{allocated_disk_gb}
        echo "Disk Size Assigned: ${allocated_disk_gb} GB"

        if command -v df >/dev/null 2>&1; then
            disk_used_output=$(df -BG "${cromwell_root}" 2>/dev/null | tail -1 | awk '{print $3}')

            if [ -n "${disk_used_output}" ]; then
                disk_used_gb=$(echo "${disk_used_output}" | sed 's/G$//')
                disk_usage_percent=$(awk -v used="$disk_used_gb" -v alloc="$allocated_disk_gb" \
                    'BEGIN { printf "%.2f", (used / alloc) * 100 }')

                echo "Final Disk Used: ${disk_used_gb} GB"
                echo "Disk Usage: ${disk_usage_percent}%"
            else
                echo "Could not measure disk usage"
            fi
        else
            echo "df command not available"
        fi

        echo ""
        echo "==========================================="
    >>>

    output {
        File final_archive = glob("*.psar")[0]
    }

    runtime {
        docker: "broadcptacdev/panoply_spectronaut:v21.0"
        cpu: cpu
        memory: "~{ram_gb}GB"
        bootDiskSizeGb: 32
        disks: "local-disk ~{allocated_disk_gb} HDD"
        preemptible: n_preemptible
        # cpuPlatform: "AMD Milan"
    }
}

task combine_final_archives {
    input {
        File analysis_schema
        Array[File] input_archives
        Int cpu
        Int ram_gb
        Int allocated_disk_gb
        Int n_preemptible
        File? enzyme_database
        File? custom_mod_repository
    }

    command <<<
        set -euo pipefail

        cromwell_root=$(pwd)

        # Resource Monitoring Initialization
        wall_start=$(date +%s%N)  # Nanoseconds since epoch

        # Cgroup V2 (modern)
        if [ -f /sys/fs/cgroup/cpu.stat ]; then
            cpu_start=$(grep "usage_usec" /sys/fs/cgroup/cpu.stat | awk '{print $2}')
            cpu_start_ns=$((cpu_start * 1000))  # Convert microseconds to nanoseconds
        # Cgroup V1 (legacy)
        elif [ -f /sys/fs/cgroup/cpuacct/cpuacct.usage ]; then
            cpu_start_ns=$(cat /sys/fs/cgroup/cpuacct/cpuacct.usage)
        else
            cpu_start_ns=0
        fi

        # Capture per-CPU stats from /proc/stat
        cpu_stat_start=$(grep "^cpu[0-9]" /proc/stat 2>/dev/null || true)

        merged_library="merged_library.kit"

        work_archives="${cromwell_root}/work_archives"
        tmp_dir="${cromwell_root}/sn_temp"
        mkdir -p "${work_archives}" "${tmp_dir}"

        echo "Copying archives for merging..."
        while IFS= read -r archive; do
            if [ -n "${archive}" ]; then
                cp "${archive}" "${work_archives}/"
            fi
        done < ~{write_lines(input_archives)}

        spectronaut \
            -setTemp "${tmp_dir}" \
            ~{if defined(enzyme_database) then "--importEnzymeDB " + enzyme_database else ""} \
            ~{if defined(custom_mod_repository) then "--importModRepository " + custom_mod_repository else ""} \
            lg -se Pulsar \
            -sad "${work_archives}" \
            -k "${cromwell_root}/${merged_library}" \
            -o "${cromwell_root}" \
            -s "~{analysis_schema}" \
            2>&1 | tee merge_archives.log

        if [ ! -f "${cromwell_root}/${merged_library}" ]; then
            echo "ERROR: Merged archive file not found" >&2
            exit 1
        fi

        echo "Archive merging complete."

        # ============================================================================
        # Resource Usage Report
        # ============================================================================
        echo "==========================================="
        echo "=== RESOURCE USAGE REPORT ==="
        echo "==========================================="

        # --- CPU Utilization ---
        echo ""
        echo "--- CPU Utilization ---"

        wall_end=$(date +%s%N)
        wall_elapsed_ns=$((wall_end - wall_start))

        # Cgroup V2
        if [ -f /sys/fs/cgroup/cpu.stat ]; then
            cpu_end=$(grep "usage_usec" /sys/fs/cgroup/cpu.stat | awk '{print $2}')
            cpu_end_ns=$((cpu_end * 1000))
        # Cgroup V1
        elif [ -f /sys/fs/cgroup/cpuacct/cpuacct.usage ]; then
            cpu_end_ns=$(cat /sys/fs/cgroup/cpuacct/cpuacct.usage)
        else
            cpu_end_ns=0
        fi

        cpu_stat_end=$(grep "^cpu[0-9]" /proc/stat 2>/dev/null || true)

        if [ "$cpu_start_ns" -gt 0 ] && [ "$cpu_end_ns" -gt "$cpu_start_ns" ] && [ "$wall_elapsed_ns" -gt 0 ]; then
            cpu_used_ns=$((cpu_end_ns - cpu_start_ns))
            cpu_utilization_total=$(awk -v cpu="$cpu_used_ns" -v wall="$wall_elapsed_ns" \
                'BEGIN { printf "%.2f", (cpu / wall) * 100 }')
            allocated_cpus=~{cpu}
            cpu_utilization_per_core=$(awk -v total="$cpu_utilization_total" -v cpus="$allocated_cpus" \
                'BEGIN { printf "%.2f", total / cpus }')

            echo "Allocated CPUs: ${allocated_cpus}"
            echo "Average CPU Utilization: ${cpu_utilization_total}% (across all cores)"

            if [ -n "$cpu_stat_start" ] && [ -n "$cpu_stat_end" ]; then
                max_core_util=$(paste \
                    <(echo "$cpu_stat_start" | awk '{idle=$5; total=0; for(i=2;i<=NF;i++) total+=$i; print idle, total}') \
                    <(echo "$cpu_stat_end"   | awk '{idle=$5; total=0; for(i=2;i<=NF;i++) total+=$i; print idle, total}') \
                    | awk '{
                        idle_start=$1; total_start=$2; idle_end=$3; total_end=$4;
                        d_total = total_end - total_start;
                        d_idle  = idle_end  - idle_start;
                        if (d_total > 0) util = (1 - d_idle/d_total) * 100; else util = 0;
                        if (util > max) max = util;
                      } END { printf "%.2f", max }')
                echo "Max Per-Core CPU Utilization: ${max_core_util}%"
            fi

            echo "Per-Core CPU Utilization: ${cpu_utilization_per_core}%"
            echo "Maximum Possible: $((allocated_cpus * 100))% (${allocated_cpus} cores at 100%)"
        else
            echo "CPU utilization data not available"
        fi

        # --- Memory Usage ---
        echo ""
        echo "--- Memory Usage ---"

        # Cgroup V2
        if [ -f /sys/fs/cgroup/memory.peak ]; then
            max_mem_bytes=$(cat /sys/fs/cgroup/memory.peak)
            limit_bytes=$(cat /sys/fs/cgroup/memory.max)
        # Cgroup V1
        elif [ -f /sys/fs/cgroup/memory/memory.max_usage_in_bytes ]; then
            max_mem_bytes=$(cat /sys/fs/cgroup/memory/memory.max_usage_in_bytes)
            limit_bytes=$(cat /sys/fs/cgroup/memory/memory.limit_in_bytes)
        else
            max_mem_bytes=0
            limit_bytes=0
        fi

        if [ "$max_mem_bytes" -gt 0 ]; then
            max_mem_gb=$(awk -v val="$max_mem_bytes" 'BEGIN { printf "%.2f", val / (1024^3) }')
            echo "Actual Peak RAM: ${max_mem_gb} GB"

            if [ "$limit_bytes" != "max" ] && [ "$limit_bytes" -gt 0 ]; then
                limit_gb=$(awk -v val="$limit_bytes" 'BEGIN { printf "%.2f", val / (1024^3) }')
                usage_percent=$(awk -v max="$max_mem_bytes" -v lim="$limit_bytes" \
                    'BEGIN { printf "%.2f", (max / lim) * 100 }')
                echo "RAM Limit: ${limit_gb} GB"
                echo "RAM Usage: ${usage_percent}%"
            fi
        else
            echo "Memory usage data not available"
        fi

        # --- Disk Usage ---
        echo ""
        echo "--- Disk Usage ---"

        allocated_disk_gb=~{allocated_disk_gb}
        echo "Disk Size Assigned: ${allocated_disk_gb} GB"

        if command -v df >/dev/null 2>&1; then
            disk_used_output=$(df -BG "${cromwell_root}" 2>/dev/null | tail -1 | awk '{print $3}')

            if [ -n "${disk_used_output}" ]; then
                disk_used_gb=$(echo "${disk_used_output}" | sed 's/G$//')
                disk_usage_percent=$(awk -v used="$disk_used_gb" -v alloc="$allocated_disk_gb" \
                    'BEGIN { printf "%.2f", (used / alloc) * 100 }')

                echo "Final Disk Used: ${disk_used_gb} GB"
                echo "Disk Usage: ${disk_usage_percent}%"
            else
                echo "Could not measure disk usage"
            fi
        else
            echo "df command not available"
        fi

        echo ""
        echo "==========================================="
    >>>

    output {
        File combined_archive_library = "merged_library.kit"
    }

    runtime {
        docker: "broadcptacdev/panoply_spectronaut:v21.0"
        cpu: cpu
        memory: "~{ram_gb}GB"
        bootDiskSizeGb: 32
        disks: "local-disk ~{allocated_disk_gb} HDD"
        preemptible: n_preemptible
        # cpuPlatform: "AMD Milan"
    }
}

task dia_analysis_binned {
    input {
        File? merged_archive
        Array[String] user_spectral_libraries
        File analysis_schema
        Array[String] input_files
        String experiment_name
        Int cpu
        Int ram_gb
        Int bin_index
        Int allocated_disk_gb
        Int n_preemptible
        File? enzyme_database
        File? custom_mod_repository
    }

    # input_files are GCS URIs (raw .d directories or converted .htrms files) and
    # user_spectral_libraries are GCS URIs (.kit files); both are downloaded inside the command
    # via gcloud storage cp -r. Marking them localizationOptional prevents Cromwell from
    # attempting to localize them as Files — which fails for timsTOF .d directories — while
    # still including their values in the call-caching hash key.
    parameter_meta {
        input_files: { localizationOptional: true }
        user_spectral_libraries: { localizationOptional: true }
    }

    command <<<
        set -euo pipefail

        cromwell_root=$(pwd)

        # Resource Monitoring Initialization
        wall_start=$(date +%s%N)  # Nanoseconds since epoch

        # Cgroup V2 (modern)
        if [ -f /sys/fs/cgroup/cpu.stat ]; then
            cpu_start=$(grep "usage_usec" /sys/fs/cgroup/cpu.stat | awk '{print $2}')
            cpu_start_ns=$((cpu_start * 1000))  # Convert microseconds to nanoseconds
        # Cgroup V1 (legacy)
        elif [ -f /sys/fs/cgroup/cpuacct/cpuacct.usage ]; then
            cpu_start_ns=$(cat /sys/fs/cgroup/cpuacct/cpuacct.usage)
        else
            cpu_start_ns=0
        fi

        # Capture per-CPU stats from /proc/stat
        cpu_stat_start=$(grep "^cpu[0-9]" /proc/stat 2>/dev/null || true)

        input_dir="${cromwell_root}/work_input"
        mkdir -p "${input_dir}"

        output_dir="${cromwell_root}/out_dia"
        mkdir -p "${output_dir}"

        tmp_dir="${cromwell_root}/work_dia_temp"
        mkdir -p "${tmp_dir}"

        # Download all input files from GCS to input directory.
        # gcloud storage cp -r handles both timsTOF .d directories and .htrms files.
        echo "Downloading input files..."
        while IFS= read -r input_file; do
            if [ -n "${input_file}" ]; then
                echo "Downloading: ${input_file}"
                gcloud storage cp -r "${input_file}" "${input_dir}/"
            fi
        done < ~{write_lines(input_files)}

        # Verify files were downloaded
        file_count=$(find "${input_dir}" -mindepth 1 -maxdepth 1 | wc -l)
        echo "Downloaded ${file_count} input item(s) to input directory for bin ~{bin_index}"

        # Download user-provided spectral libraries from GCS
        user_lib_dir="${cromwell_root}/user_libraries"
        mkdir -p "${user_lib_dir}"
        user_lib_args=""
        while IFS= read -r lib_path; do
            if [ -n "${lib_path}" ]; then
                echo "Downloading user spectral library: ${lib_path}"
                gcloud storage cp -r "${lib_path}" "${user_lib_dir}/"
                local_lib="${user_lib_dir}/$(basename "${lib_path}")"
                user_lib_args="${user_lib_args} -a ${local_lib}"
            fi
        done < ~{write_lines(user_spectral_libraries)}

        # Validate that at least one spectral library is available
        if [ "~{defined(merged_archive)}" = "false" ] && [ -z "${user_lib_args}" ]; then
            echo "ERROR: No spectral library available for bin ~{bin_index}. Either merged_archive or user_spectral_libraries is required." >&2
            exit 1
        fi

        # Run DIA analysis on ALL files in bin at once (batch processing)
        echo "Running DIA analysis for bin ~{bin_index} (batch processing ${file_count} files)..."
        spectronaut \
            -setTemp "${tmp_dir}" \
            ~{if defined(enzyme_database) then "--importEnzymeDB " + enzyme_database else ""} \
            ~{if defined(custom_mod_repository) then "--importModRepository " + custom_mod_repository else ""} \
            diaanalysis \
            -s "~{analysis_schema}" \
            -n "~{experiment_name}_bin_~{bin_index}" \
            -o "${output_dir}" \
            -d "${input_dir}" \
            ~{if defined(merged_archive) then "-a " + merged_archive else ""} \
            ${user_lib_args} \
            2>&1 | tee dia_analysis_bin_~{bin_index}.log

        # Find all .sne files produced
        sne_count=$(find "${output_dir}" -type f -name "*.sne" | wc -l)
        if [ "${sne_count}" -eq 0 ]; then
            echo "ERROR: No .sne files produced for bin ~{bin_index}" >&2
            exit 1
        fi

        # Move all .sne files to cromwell root
        echo "Moving ${sne_count} SNE files..."
        find "${output_dir}" -type f -name "*.sne" -exec mv {} "${cromwell_root}/" \;

        echo "DIA analysis complete for bin ~{bin_index}. Generated ${sne_count} SNE files."

        # ============================================================================
        # Resource Usage Report
        # ============================================================================
        echo "==========================================="
        echo "=== RESOURCE USAGE REPORT ==="
        echo "==========================================="

        # --- CPU Utilization ---
        echo ""
        echo "--- CPU Utilization ---"

        wall_end=$(date +%s%N)
        wall_elapsed_ns=$((wall_end - wall_start))

        # Cgroup V2
        if [ -f /sys/fs/cgroup/cpu.stat ]; then
            cpu_end=$(grep "usage_usec" /sys/fs/cgroup/cpu.stat | awk '{print $2}')
            cpu_end_ns=$((cpu_end * 1000))
        # Cgroup V1
        elif [ -f /sys/fs/cgroup/cpuacct/cpuacct.usage ]; then
            cpu_end_ns=$(cat /sys/fs/cgroup/cpuacct/cpuacct.usage)
        else
            cpu_end_ns=0
        fi

        cpu_stat_end=$(grep "^cpu[0-9]" /proc/stat 2>/dev/null || true)

        if [ "$cpu_start_ns" -gt 0 ] && [ "$cpu_end_ns" -gt "$cpu_start_ns" ] && [ "$wall_elapsed_ns" -gt 0 ]; then
            cpu_used_ns=$((cpu_end_ns - cpu_start_ns))
            cpu_utilization_total=$(awk -v cpu="$cpu_used_ns" -v wall="$wall_elapsed_ns" \
                'BEGIN { printf "%.2f", (cpu / wall) * 100 }')
            allocated_cpus=~{cpu}
            cpu_utilization_per_core=$(awk -v total="$cpu_utilization_total" -v cpus="$allocated_cpus" \
                'BEGIN { printf "%.2f", total / cpus }')

            echo "Allocated CPUs: ${allocated_cpus}"
            echo "Average CPU Utilization: ${cpu_utilization_total}% (across all cores)"

            if [ -n "$cpu_stat_start" ] && [ -n "$cpu_stat_end" ]; then
                max_core_util=$(paste \
                    <(echo "$cpu_stat_start" | awk '{idle=$5; total=0; for(i=2;i<=NF;i++) total+=$i; print idle, total}') \
                    <(echo "$cpu_stat_end"   | awk '{idle=$5; total=0; for(i=2;i<=NF;i++) total+=$i; print idle, total}') \
                    | awk '{
                        idle_start=$1; total_start=$2; idle_end=$3; total_end=$4;
                        d_total = total_end - total_start;
                        d_idle  = idle_end  - idle_start;
                        if (d_total > 0) util = (1 - d_idle/d_total) * 100; else util = 0;
                        if (util > max) max = util;
                      } END { printf "%.2f", max }')
                echo "Max Per-Core CPU Utilization: ${max_core_util}%"
            fi

            echo "Per-Core CPU Utilization: ${cpu_utilization_per_core}%"
            echo "Maximum Possible: $((allocated_cpus * 100))% (${allocated_cpus} cores at 100%)"
        else
            echo "CPU utilization data not available"
        fi

        # --- Memory Usage ---
        echo ""
        echo "--- Memory Usage ---"

        # Cgroup V2
        if [ -f /sys/fs/cgroup/memory.peak ]; then
            max_mem_bytes=$(cat /sys/fs/cgroup/memory.peak)
            limit_bytes=$(cat /sys/fs/cgroup/memory.max)
        # Cgroup V1
        elif [ -f /sys/fs/cgroup/memory/memory.max_usage_in_bytes ]; then
            max_mem_bytes=$(cat /sys/fs/cgroup/memory/memory.max_usage_in_bytes)
            limit_bytes=$(cat /sys/fs/cgroup/memory/memory.limit_in_bytes)
        else
            max_mem_bytes=0
            limit_bytes=0
        fi

        if [ "$max_mem_bytes" -gt 0 ]; then
            max_mem_gb=$(awk -v val="$max_mem_bytes" 'BEGIN { printf "%.2f", val / (1024^3) }')
            echo "Actual Peak RAM: ${max_mem_gb} GB"

            if [ "$limit_bytes" != "max" ] && [ "$limit_bytes" -gt 0 ]; then
                limit_gb=$(awk -v val="$limit_bytes" 'BEGIN { printf "%.2f", val / (1024^3) }')
                usage_percent=$(awk -v max="$max_mem_bytes" -v lim="$limit_bytes" \
                    'BEGIN { printf "%.2f", (max / lim) * 100 }')
                echo "RAM Limit: ${limit_gb} GB"
                echo "RAM Usage: ${usage_percent}%"
            fi
        else
            echo "Memory usage data not available"
        fi

        # --- Disk Usage ---
        echo ""
        echo "--- Disk Usage ---"

        allocated_disk_gb=~{allocated_disk_gb}
        echo "Disk Size Assigned: ${allocated_disk_gb} GB"

        if command -v df >/dev/null 2>&1; then
            disk_used_output=$(df -BG "${cromwell_root}" 2>/dev/null | tail -1 | awk '{print $3}')

            if [ -n "${disk_used_output}" ]; then
                disk_used_gb=$(echo "${disk_used_output}" | sed 's/G$//')
                disk_usage_percent=$(awk -v used="$disk_used_gb" -v alloc="$allocated_disk_gb" \
                    'BEGIN { printf "%.2f", (used / alloc) * 100 }')

                echo "Final Disk Used: ${disk_used_gb} GB"
                echo "Disk Usage: ${disk_usage_percent}%"
            else
                echo "Could not measure disk usage"
            fi
        else
            echo "df command not available"
        fi

        echo ""
        echo "==========================================="
    >>>

    output {
        Array[File] sne_files = glob("*.sne")
    }

    runtime {
        docker: "broadcptacdev/panoply_spectronaut:v21.0"
        cpu: cpu
        memory: "~{ram_gb}GB"
        bootDiskSizeGb: 32
        disks: "local-disk ~{allocated_disk_gb} HDD"
        preemptible: n_preemptible
        # cpuPlatform: "AMD Milan"
    }
}

task combine_sne {
    input {
        File analysis_schema
        Array[File] sne_files
        String experiment_name
        Int ram_gb
        Int cpu
        Int allocated_disk_gb
        Int n_preemptible
        File? condition_setup
        File? report_schema_1
        File? report_schema_2
        File? report_schema_3
        File? report_schema_4
        File? enzyme_database
        File? custom_mod_repository
        Boolean generate_sne_large_experiment
    }

    command <<<
        set -euo pipefail

        cromwell_root=$(pwd)

        # Resource Monitoring Initialization
        wall_start=$(date +%s%N)  # Nanoseconds since epoch

        # Cgroup V2 (modern)
        if [ -f /sys/fs/cgroup/cpu.stat ]; then
            cpu_start=$(grep "usage_usec" /sys/fs/cgroup/cpu.stat | awk '{print $2}')
            cpu_start_ns=$((cpu_start * 1000))  # Convert microseconds to nanoseconds
        # Cgroup V1 (legacy)
        elif [ -f /sys/fs/cgroup/cpuacct/cpuacct.usage ]; then
            cpu_start_ns=$(cat /sys/fs/cgroup/cpuacct/cpuacct.usage)
        else
            cpu_start_ns=0
        fi

        # Capture per-CPU stats from /proc/stat
        cpu_stat_start=$(grep "^cpu[0-9]" /proc/stat 2>/dev/null || true)

        output_dir="${cromwell_root}/out_combine"
        output_zip="${cromwell_root}/spectronaut_output.zip"
        tmp_dir="${cromwell_root}/sn_temp"

        mkdir -p "${output_dir}" "${tmp_dir}"

        sne_dir="${cromwell_root}/work_snes"
        mkdir -p "${sne_dir}"

        echo "Copying SNE files for merging..."
        while IFS= read -r sne_file; do
            if [ -n "${sne_file}" ]; then
                cp "${sne_file}" "${sne_dir}/"
            fi
        done < ~{write_lines(sne_files)}

        if [ "~{generate_sne_large_experiment}" = "true" ]; then
            spectronaut \
                -setTemp "${tmp_dir}" \
                ~{if defined(enzyme_database) then "--importEnzymeDB " + enzyme_database else ""} \
                ~{if defined(custom_mod_repository) then "--importModRepository " + custom_mod_repository else ""} \
                manageSNE --merge \
                -n "~{experiment_name}" \
                -o "${output_dir}" \
                -d "${sne_dir}" \
                ~{if defined(condition_setup) then "-con " + condition_setup else ""} \
                -s "~{analysis_schema}" \
                ~{if defined(report_schema_1) then "-rs " + report_schema_1 else ""} \
                ~{if defined(report_schema_2) then "-rs " + report_schema_2 else ""} \
                ~{if defined(report_schema_3) then "-rs " + report_schema_3 else ""} \
                ~{if defined(report_schema_4) then "-rs " + report_schema_4 else ""} \
                2>&1 | tee spectronaut_combine.log
        else
            spectronaut \
                -setTemp "${tmp_dir}" \
                ~{if defined(enzyme_database) then "--importEnzymeDB " + enzyme_database else ""} \
                ~{if defined(custom_mod_repository) then "--importModRepository " + custom_mod_repository else ""} \
                combine \
                -n "~{experiment_name}" \
                -o "${output_dir}" \
                -d "${sne_dir}" \
                ~{if defined(condition_setup) then "-con " + condition_setup else ""} \
                -s "~{analysis_schema}" \
                ~{if defined(report_schema_1) then "-rs " + report_schema_1 else ""} \
                ~{if defined(report_schema_2) then "-rs " + report_schema_2 else ""} \
                ~{if defined(report_schema_3) then "-rs " + report_schema_3 else ""} \
                ~{if defined(report_schema_4) then "-rs " + report_schema_4 else ""} \
                2>&1 | tee spectronaut_combine.log
        fi

        echo "Creating output archive..."
        zip -r "${output_zip}" "${output_dir}" -x \*.zip

        if [ ! -f "${output_zip}" ]; then
            echo "ERROR: Failed to create output zip file" >&2
            exit 1
        fi

        echo "SNE merging complete."

        # ============================================================================
        # Resource Usage Report
        # ============================================================================
        echo "==========================================="
        echo "=== RESOURCE USAGE REPORT ==="
        echo "==========================================="

        # --- CPU Utilization ---
        echo ""
        echo "--- CPU Utilization ---"

        wall_end=$(date +%s%N)
        wall_elapsed_ns=$((wall_end - wall_start))

        # Cgroup V2
        if [ -f /sys/fs/cgroup/cpu.stat ]; then
            cpu_end=$(grep "usage_usec" /sys/fs/cgroup/cpu.stat | awk '{print $2}')
            cpu_end_ns=$((cpu_end * 1000))
        # Cgroup V1
        elif [ -f /sys/fs/cgroup/cpuacct/cpuacct.usage ]; then
            cpu_end_ns=$(cat /sys/fs/cgroup/cpuacct/cpuacct.usage)
        else
            cpu_end_ns=0
        fi

        cpu_stat_end=$(grep "^cpu[0-9]" /proc/stat 2>/dev/null || true)

        if [ "$cpu_start_ns" -gt 0 ] && [ "$cpu_end_ns" -gt "$cpu_start_ns" ] && [ "$wall_elapsed_ns" -gt 0 ]; then
            cpu_used_ns=$((cpu_end_ns - cpu_start_ns))
            cpu_utilization_total=$(awk -v cpu="$cpu_used_ns" -v wall="$wall_elapsed_ns" \
                'BEGIN { printf "%.2f", (cpu / wall) * 100 }')
            allocated_cpus=~{cpu}
            cpu_utilization_per_core=$(awk -v total="$cpu_utilization_total" -v cpus="$allocated_cpus" \
                'BEGIN { printf "%.2f", total / cpus }')

            echo "Allocated CPUs: ${allocated_cpus}"
            echo "Average CPU Utilization: ${cpu_utilization_total}% (across all cores)"

            if [ -n "$cpu_stat_start" ] && [ -n "$cpu_stat_end" ]; then
                max_core_util=$(paste \
                    <(echo "$cpu_stat_start" | awk '{idle=$5; total=0; for(i=2;i<=NF;i++) total+=$i; print idle, total}') \
                    <(echo "$cpu_stat_end"   | awk '{idle=$5; total=0; for(i=2;i<=NF;i++) total+=$i; print idle, total}') \
                    | awk '{
                        idle_start=$1; total_start=$2; idle_end=$3; total_end=$4;
                        d_total = total_end - total_start;
                        d_idle  = idle_end  - idle_start;
                        if (d_total > 0) util = (1 - d_idle/d_total) * 100; else util = 0;
                        if (util > max) max = util;
                      } END { printf "%.2f", max }')
                echo "Max Per-Core CPU Utilization: ${max_core_util}%"
            fi

            echo "Per-Core CPU Utilization: ${cpu_utilization_per_core}%"
            echo "Maximum Possible: $((allocated_cpus * 100))% (${allocated_cpus} cores at 100%)"
        else
            echo "CPU utilization data not available"
        fi

        # --- Memory Usage ---
        echo ""
        echo "--- Memory Usage ---"

        # Cgroup V2
        if [ -f /sys/fs/cgroup/memory.peak ]; then
            max_mem_bytes=$(cat /sys/fs/cgroup/memory.peak)
            limit_bytes=$(cat /sys/fs/cgroup/memory.max)
        # Cgroup V1
        elif [ -f /sys/fs/cgroup/memory/memory.max_usage_in_bytes ]; then
            max_mem_bytes=$(cat /sys/fs/cgroup/memory/memory.max_usage_in_bytes)
            limit_bytes=$(cat /sys/fs/cgroup/memory/memory.limit_in_bytes)
        else
            max_mem_bytes=0
            limit_bytes=0
        fi

        if [ "$max_mem_bytes" -gt 0 ]; then
            max_mem_gb=$(awk -v val="$max_mem_bytes" 'BEGIN { printf "%.2f", val / (1024^3) }')
            echo "Actual Peak RAM: ${max_mem_gb} GB"

            if [ "$limit_bytes" != "max" ] && [ "$limit_bytes" -gt 0 ]; then
                limit_gb=$(awk -v val="$limit_bytes" 'BEGIN { printf "%.2f", val / (1024^3) }')
                usage_percent=$(awk -v max="$max_mem_bytes" -v lim="$limit_bytes" \
                    'BEGIN { printf "%.2f", (max / lim) * 100 }')
                echo "RAM Limit: ${limit_gb} GB"
                echo "RAM Usage: ${usage_percent}%"
            fi
        else
            echo "Memory usage data not available"
        fi

        # --- Disk Usage ---
        echo ""
        echo "--- Disk Usage ---"

        allocated_disk_gb=~{allocated_disk_gb}
        echo "Disk Size Assigned: ${allocated_disk_gb} GB"

        if command -v df >/dev/null 2>&1; then
            disk_used_output=$(df -BG "${cromwell_root}" 2>/dev/null | tail -1 | awk '{print $3}')

            if [ -n "${disk_used_output}" ]; then
                disk_used_gb=$(echo "${disk_used_output}" | sed 's/G$//')
                disk_usage_percent=$(awk -v used="$disk_used_gb" -v alloc="$allocated_disk_gb" \
                    'BEGIN { printf "%.2f", (used / alloc) * 100 }')

                echo "Final Disk Used: ${disk_used_gb} GB"
                echo "Disk Usage: ${disk_usage_percent}%"
            else
                echo "Could not measure disk usage"
            fi
        else
            echo "df command not available"
        fi

        echo ""
        echo "==========================================="
    >>>

    output {
        File spectronaut_output = "spectronaut_output.zip"
    }

    runtime {
        docker: "broadcptacdev/panoply_spectronaut:v21.0"
        cpu: cpu
        memory: "~{ram_gb}GB"
        bootDiskSizeGb: 32
        disks: "local-disk ~{allocated_disk_gb} HDD"
        preemptible: n_preemptible
        # cpuPlatform: "AMD Milan"
    }
}

task validate_skip_pulsar {
    input {
        Boolean skip_pulsar
        Boolean has_library
    }

    command <<<
        set -euo pipefail
        if [ "~{skip_pulsar}" = "true" ] && [ "~{has_library}" = "false" ]; then
            echo "ERROR: skip_pulsar=true requires at least one spectral library." >&2
            echo "Provide spectral_library_1, spectral_library_2, or spectral_library_3." >&2
            exit 1
        fi
        echo "Validation passed."
    >>>

    output {
        String validation_status = read_string(stdout())
    }

    runtime {
        docker: "ubuntu:22.04"
        cpu: 1
        memory: "1GB"
        bootDiskSizeGb: 20
        disks: "local-disk 10 HDD"
        preemptible: 3
    }
}

task sum_floats {
    input {
        Array[Float] sizes
    }

    command <<<
        python3 <<CODE
        with open("~{write_lines(sizes)}") as f:
            sizes = [float(line.strip()) for line in f if line.strip()]
        print(sum(sizes))
        CODE
    >>>

    output {
        Float total_size = read_float(stdout())
    }

    runtime {
        docker: "python:3.9-slim"
        cpu: 4
        memory: "16GB"
        preemptible: 2
        bootDiskSizeGb: 20
        disks: "local-disk 100 HDD"
    }
}

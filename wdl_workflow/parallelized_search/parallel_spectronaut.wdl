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
#   QC Mode (search_qc=true):
#     One sample per VM: independent directDIA search per sample, then a single
#     SNE merge. num_vms, do_pulsar and spectral_library_* are ignored.
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
        Int n_preemptible_search_qc = 2  # QC-mode per-sample search preemptible attempts
        Boolean generate_sne_large_experiment = true  # If true, use manageSNE --merge; if false, use spectronaut combine

        # ============================================================================
        # Spectral Library Bypass Configuration
        # ============================================================================
        Boolean do_pulsar = true  # Run Pulsar steps; set to false to skip Pulsar and use user-provided libraries directly
        String? spectral_library_1   # Optional user-provided spectral library (.kit file, GCS path)
        String? spectral_library_2   # Optional user-provided spectral library (.kit file, GCS path)
        String? spectral_library_3   # Optional user-provided spectral library (.kit file, GCS path)

        # ============================================================================
        # QC Search Mode Configuration
        # ============================================================================
        # When true, every sample is searched independently by directDIA on its own VM
        # and the per-sample SNE files are merged into one experiment. num_vms, do_pulsar
        # and spectral_library_* are ignored: QC is method evaluation against a library
        # generated from each sample itself via Pulsar Search.
        Boolean search_qc = false
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
        search_qc = search_qc,
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
    if (num_vms > 1 && !search_qc) {
        call create_bins { input:
            file_paths = files_for_search,
            num_bins = num_vms,
        }
    }

    Float finalized_total_size_gb = select_first([
        sum_floats.total_size,
        total_input_size_gb,
    ])

    # Shared RAM ceiling for dynamically-sized non-parallelized tasks (conservative
    # against Compute Engine machine-type limits)
    Int dynamic_ram_cap_gb = 700

    # Fixed per-shard disk for QC-mode search VMs. Each VM handles a single sample, so
    # the allocation is deliberately generous rather than size-derived, leaving headroom
    # for Spectronaut temp files on any single-run search.
    Int qc_search_disk_gb = 800

    # Floor for the QC-mode SNE combine disk allocation
    Int qc_combine_disk_floor_gb = 1000

    # Compute non-parallelized task RAM based on total file count (floor of 128 GB, capped at dynamic_ram_cap_gb)
    Int pulsar_step2_ram_gb = if (ceil(pulsar_step2_base_ram_per_file * list_files.num_files) > dynamic_ram_cap_gb)
        then dynamic_ram_cap_gb
        else if (ceil(pulsar_step2_base_ram_per_file * list_files.num_files) > 128)
            then ceil(pulsar_step2_base_ram_per_file * list_files.num_files)
            else 128

    Int combine_archives_ram_gb = if (ceil(combine_archives_base_ram_per_file * list_files.num_files) > dynamic_ram_cap_gb)
        then dynamic_ram_cap_gb
        else if (ceil(combine_archives_base_ram_per_file * list_files.num_files) > 128)
            then ceil(combine_archives_base_ram_per_file * list_files.num_files)
            else 128

    Float combine_sne_ram_per_file = if generate_sne_large_experiment then combine_sne_ram_per_file_merge else combine_sne_ram_per_file_combine
    Int combine_sne_ram_gb = if ceil(combine_sne_ram_per_file * list_files.num_files) > dynamic_ram_cap_gb
        then dynamic_ram_cap_gb
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

    Int calculated_num_vms = if (num_vms > 1 && !search_qc) then read_int(select_first([
        create_bins.calculated_num_vms_file,
    ])) else 1

    Array[Array[String]] search_file_bins = if (num_vms > 1 && !search_qc) then read_json(select_first([
        create_bins.bins_json,
    ])) else []

    # BRANCH A: Single VM Mode (num_vms == 1, search_qc = false)
    if (!search_qc && calculated_num_vms == 1) {
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

    # BRANCH B: Parallel Mode (num_vms > 1, search_qc = false)
    if (!search_qc && calculated_num_vms > 1) {

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

    # BRANCH C: QC Mode (search_qc = true)
    # Each sample is searched independently by directDIA on its own VM, then every
    # per-sample SNE file is merged into a single combined experiment. num_vms is ignored
    # (QC always allocates one VM per sample), as are do_pulsar and spectral_library_*:
    # QC is method evaluation against a library generated from each sample itself.
    if (search_qc) {

        # ========================================================================
        # QC STEP 1: Per-sample directDIA search (one sample per VM)
        # ========================================================================
        scatter (i in range(length(files_for_search))) {
            # Strip the run extension so the experiment name stays traceable to its
            # source file, and carry the shard index so the name is unique by
            # construction. Uniqueness is required, not cosmetic: combine_sne copies
            # every SNE into one flat directory, so two shards sharing a name would
            # silently drop a sample from the merge. The stem alone is not enough —
            # a directory holding both S1.raw and S1.d yields the same stem, and with
            # do_conversion=true both convert to an identically-named S1.htrms.
            String qc_sample_id = sub(basename(files_for_search[i]), "\\.(d|raw|RAW|htrms)$", "")

            call qc_directDIA_single_sample { input:
                input_files = [files_for_search[i]],
                experiment_name = experiment_name + "_qc_" + i + "_" + qc_sample_id,
                analysis_schema = directDIA_settings,
                fasta_1 = fasta_1,
                fasta_2 = fasta_2,
                fasta_3 = fasta_3,
                enzyme_database = enzyme_database,
                custom_mod_repository = custom_mod_repository,
                json_settings = json_settings,
                cpu = directDIA_single_vm_cpu,
                ram_gb = directDIA_single_vm_ram_gb,
                allocated_disk_gb = qc_search_disk_gb,
                n_preemptible = n_preemptible_search_qc,
            }
        }

        Array[File] qc_sne = flatten(qc_directDIA_single_sample.sne_files)

        # Disk for the QC merge: total input size scaled by the SNE multiplier, floored
        # so small QC sets still get workable space.
        Int qc_combine_disk_gb = if ceil(finalized_total_size_gb * sne_combine_disk_size_multiplier) > qc_combine_disk_floor_gb
            then ceil(finalized_total_size_gb * sne_combine_disk_size_multiplier)
            else qc_combine_disk_floor_gb

        # ========================================================================
        # QC STEP 2: Merge every per-sample SNE and generate reports
        # ========================================================================
        call combine_sne as combine_sne_qc { input:
            experiment_name = experiment_name,
            sne_files = qc_sne,
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
            allocated_disk_gb = qc_combine_disk_gb,
            generate_sne_large_experiment = generate_sne_large_experiment,
        }
    }

    # Final output: select from the parallel, QC or single-VM path. The three branches are
    # mutually exclusive, so exactly one of these is defined on any given run.
    output {
        File spectronaut_output = select_first([
            combine_sne.spectronaut_output,
            combine_sne_qc.spectronaut_output,
            directDIA_single_vm.spectronaut_output,
        ])
        # Per-sample SNE files from QC mode (undefined on non-QC runs). Exposed so a
        # subset can be re-merged with wdl_workflow/spectronaut_modules/sne_combine.wdl
        # without re-running any search.
        Array[File]? qc_sne_files = qc_sne
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
        # shellcheck source=/dev/null
        source /usr/local/bin/sn_resource_monitor.sh
        # shellcheck source=/dev/null
        source /usr/local/bin/sn_download.sh
        # shellcheck source=/dev/null
        source /usr/local/bin/sn_import_flags.sh

        sn_monitor_start

        input_dir="${cromwell_root}/work_input"
        mkdir -p "${input_dir}"

        output_dir="${cromwell_root}/out_spectronaut"
        mkdir -p "${output_dir}"

        tmp_dir="${cromwell_root}/sn_temp"
        mkdir -p "${tmp_dir}"

        output_zip="${cromwell_root}/spectronaut_output.zip"

        # Download all input files from GCS to input directory.
        # gcloud storage cp -r handles both timsTOF .d directories and .htrms files.
        sn_download_inputs "${input_dir}" ~{write_lines(input_files)}

        # Download user spectral libraries (if any) from GCS
        user_lib_dir="${cromwell_root}/user_libraries"
        user_lib_args="$(sn_download_libraries "${user_lib_dir}" ~{write_lines(user_spectral_libraries)})"

        export ENZYME_DB="~{if defined(enzyme_database) then enzyme_database else ''}"
        export MOD_REPO="~{if defined(custom_mod_repository) then custom_mod_repository else ''}"
        import_flags="$(sn_build_import_flags)"

        # Run spectronaut search
        if [ "~{skip_pulsar}" = "true" ]; then
            # do_pulsar=false: use diaanalysis command with user-provided spectral libraries
            echo "Starting DIA analysis (do_pulsar=false)..."
            spectronaut \
                -setTemp "${tmp_dir}" \
                ${import_flags} \
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
                ${import_flags} \
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

        sn_monitor_report ~{cpu} ~{allocated_disk_gb} "${cromwell_root}"
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

task qc_directDIA_single_sample {
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
        File? custom_mod_repository
        File? json_settings
    }

    # input_files holds exactly one GCS URI — a raw timsTOF .d directory, a .raw file, or
    # a converted .htrms file — downloaded inside the command via gcloud storage cp -r.
    # Marking it localizationOptional prevents Cromwell from attempting to localize it as
    # a File, which fails for .d directories, while still including its value in the
    # call-caching hash key.
    parameter_meta {
        input_files: { localizationOptional: true }
    }

    command <<<
        set -euo pipefail

        cromwell_root=$(pwd)
        # shellcheck source=/dev/null
        source /usr/local/bin/sn_resource_monitor.sh
        # shellcheck source=/dev/null
        source /usr/local/bin/sn_download.sh
        # shellcheck source=/dev/null
        source /usr/local/bin/sn_import_flags.sh

        sn_monitor_start

        input_dir="${cromwell_root}/work_input"
        output_dir="${cromwell_root}/out_qc_search"
        tmp_dir="${cromwell_root}/sn_temp"
        mkdir -p "${output_dir}" "${tmp_dir}"

        # Download this shard's single sample. gcloud storage cp -r handles timsTOF .d
        # directories and single-file formats (.raw / .htrms) alike.
        sn_download_inputs "${input_dir}" ~{write_lines(input_files)}

        echo "Files in input directory:"
        ls -1 "${input_dir}"

        export ENZYME_DB="~{if defined(enzyme_database) then enzyme_database else ''}"
        export MOD_REPO="~{if defined(custom_mod_repository) then custom_mod_repository else ''}"
        import_flags="$(sn_build_import_flags)"

        # -d adds every Spectronaut-recognized run in a directory, including vendor
        # formats represented as folders (Bruker .d, Waters). Pointing it at the download
        # directory makes this task format-agnostic: no per-extension flag detection.
        echo "Starting QC directDIA search for ~{experiment_name}..."
        spectronaut \
            -setTemp "${tmp_dir}" \
            ${import_flags} \
            direct \
            -s "~{analysis_schema}" \
            -fasta "~{fasta_1}" \
            ~{if defined(fasta_2) then "-fasta " + fasta_2 else ""} \
            ~{if defined(fasta_3) then "-fasta " + fasta_3 else ""} \
            ~{if defined(json_settings) then "-j " + json_settings else ""} \
            -n "~{experiment_name}" \
            -o "${output_dir}" \
            -d "${input_dir}" \
            2>&1 | tee qc_search.log

        sne_count=$(find "${output_dir}" -type f -name "*.sne" | wc -l)
        if [ "${sne_count}" -eq 0 ]; then
            echo "ERROR: No .sne file produced for ~{experiment_name}" >&2
            echo "Output directory contents:" >&2
            ls -lhR "${output_dir}" >&2
            exit 1
        fi

        echo "Moving ${sne_count} SNE file(s)..."
        find "${output_dir}" -type f -name "*.sne" -exec mv {} "${cromwell_root}/" \;
        echo "QC directDIA search complete for ~{experiment_name}."

        sn_monitor_report ~{cpu} ~{allocated_disk_gb} "${cromwell_root}"
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
        # shellcheck source=/dev/null
        source /usr/local/bin/sn_resource_monitor.sh
        # shellcheck source=/dev/null
        source /usr/local/bin/sn_download.sh
        # shellcheck source=/dev/null
        source /usr/local/bin/sn_import_flags.sh
        # shellcheck source=/dev/null
        source /usr/local/bin/sn_input_flags.sh

        sn_monitor_start

        input_dir="${cromwell_root}/work_input"
        output_dir="${cromwell_root}/out_pulsar_step1"
        tmp_dir="${cromwell_root}/sn_temp"
        mkdir -p "${output_dir}" "${tmp_dir}"

        sn_download_inputs "${input_dir}" ~{write_lines(input_files)}

        echo "Files in input directory:"
        ls -1 "${input_dir}"

        # HTRMS files require -r per file; raw files (incl. timsTOF .d dirs) use -d directory.
        cmd_flags="$(sn_build_input_flags "${input_dir}")"

        export ENZYME_DB="~{if defined(enzyme_database) then enzyme_database else ''}"
        export MOD_REPO="~{if defined(custom_mod_repository) then custom_mod_repository else ''}"
        import_flags="$(sn_build_import_flags)"

        spectronaut \
            -setTemp "${tmp_dir}" \
            ${import_flags} \
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

        sn_monitor_report ~{cpu} ~{allocated_disk_gb} "${cromwell_root}"
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
        # shellcheck source=/dev/null
        source /usr/local/bin/sn_resource_monitor.sh
        # shellcheck source=/dev/null
        source /usr/local/bin/sn_import_flags.sh

        sn_monitor_start

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
        export ENZYME_DB="~{if defined(enzyme_database) then enzyme_database else ''}"
        export MOD_REPO="~{if defined(custom_mod_repository) then custom_mod_repository else ''}"
        import_flags="$(sn_build_import_flags)"

        spectronaut \
            -setTemp "${tmp_dir}" \
            ${import_flags} \
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

        sn_monitor_report ~{cpu} ~{allocated_disk_gb} "${cromwell_root}"
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
        # shellcheck source=/dev/null
        source /usr/local/bin/sn_resource_monitor.sh
        # shellcheck source=/dev/null
        source /usr/local/bin/sn_download.sh
        # shellcheck source=/dev/null
        source /usr/local/bin/sn_import_flags.sh
        # shellcheck source=/dev/null
        source /usr/local/bin/sn_input_flags.sh

        sn_monitor_start

        input_dir="${cromwell_root}/work_input"
        output_dir="${cromwell_root}/out_pulsar_step3"
        tmp_dir="${cromwell_root}/sn_temp"
        mkdir -p "${output_dir}" "${tmp_dir}"

        sn_download_inputs "${input_dir}" ~{write_lines(input_files)}

        # Run Pulsar Step 3 with optimized models (batch processing)
        echo "Running Pulsar Step 3 for bin ~{bin_index} with optimized models..."

        # List files for troubleshooting
        echo "Files in input directory:"
        ls -1 "${input_dir}"

        # Construct input flags based on actual file type:
        # HTRMS files require -r per file; raw files (incl. timsTOF .d directories) use -d directory.
        # Detect HTRMS by top-level .htrms entries so that files inside a .d directory are ignored.
        cmd_flags="$(sn_build_input_flags "${input_dir}")"

        export ENZYME_DB="~{if defined(enzyme_database) then enzyme_database else ''}"
        export MOD_REPO="~{if defined(custom_mod_repository) then custom_mod_repository else ''}"
        import_flags="$(sn_build_import_flags)"

        spectronaut \
            -setTemp "${tmp_dir}" \
            ${import_flags} \
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

        sn_monitor_report ~{cpu} ~{allocated_disk_gb} "${cromwell_root}"
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
        # shellcheck source=/dev/null
        source /usr/local/bin/sn_resource_monitor.sh
        # shellcheck source=/dev/null
        source /usr/local/bin/sn_import_flags.sh

        sn_monitor_start

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

        export ENZYME_DB="~{if defined(enzyme_database) then enzyme_database else ''}"
        export MOD_REPO="~{if defined(custom_mod_repository) then custom_mod_repository else ''}"
        import_flags="$(sn_build_import_flags)"

        spectronaut \
            -setTemp "${tmp_dir}" \
            ${import_flags} \
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

        sn_monitor_report ~{cpu} ~{allocated_disk_gb} "${cromwell_root}"
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
        # shellcheck source=/dev/null
        source /usr/local/bin/sn_resource_monitor.sh
        # shellcheck source=/dev/null
        source /usr/local/bin/sn_download.sh
        # shellcheck source=/dev/null
        source /usr/local/bin/sn_import_flags.sh

        sn_monitor_start

        input_dir="${cromwell_root}/work_input"
        mkdir -p "${input_dir}"

        output_dir="${cromwell_root}/out_dia"
        mkdir -p "${output_dir}"

        tmp_dir="${cromwell_root}/work_dia_temp"
        mkdir -p "${tmp_dir}"

        # Download all input files from GCS to input directory.
        # gcloud storage cp -r handles both timsTOF .d directories and .htrms files.
        sn_download_inputs "${input_dir}" ~{write_lines(input_files)}
        file_count=$(find "${input_dir}" -mindepth 1 -maxdepth 1 | wc -l)

        # Download user-provided spectral libraries from GCS
        user_lib_dir="${cromwell_root}/user_libraries"
        user_lib_args="$(sn_download_libraries "${user_lib_dir}" ~{write_lines(user_spectral_libraries)})"

        # Validate that at least one spectral library is available
        if [ "~{defined(merged_archive)}" = "false" ] && [ -z "${user_lib_args}" ]; then
            echo "ERROR: No spectral library available for bin ~{bin_index}. Either merged_archive or user_spectral_libraries is required." >&2
            exit 1
        fi

        export ENZYME_DB="~{if defined(enzyme_database) then enzyme_database else ''}"
        export MOD_REPO="~{if defined(custom_mod_repository) then custom_mod_repository else ''}"
        import_flags="$(sn_build_import_flags)"

        # Run DIA analysis on ALL files in bin at once (batch processing)
        echo "Running DIA analysis for bin ~{bin_index} (batch processing ${file_count} files)..."
        spectronaut \
            -setTemp "${tmp_dir}" \
            ${import_flags} \
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

        sn_monitor_report ~{cpu} ~{allocated_disk_gb} "${cromwell_root}"
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
        # shellcheck source=/dev/null
        source /usr/local/bin/sn_resource_monitor.sh
        # shellcheck source=/dev/null
        source /usr/local/bin/sn_import_flags.sh

        sn_monitor_start

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

        export ENZYME_DB="~{if defined(enzyme_database) then enzyme_database else ''}"
        export MOD_REPO="~{if defined(custom_mod_repository) then custom_mod_repository else ''}"
        import_flags="$(sn_build_import_flags)"

        if [ "~{generate_sne_large_experiment}" = "true" ]; then
            spectronaut \
                -setTemp "${tmp_dir}" \
                ${import_flags} \
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
                ${import_flags} \
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

        sn_monitor_report ~{cpu} ~{allocated_disk_gb} "${cromwell_root}"
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
        Boolean search_qc
    }

    command <<<
        set -euo pipefail
        # QC mode runs per-sample directDIA against a library generated from each sample
        # itself, so do_pulsar and spectral_library_* are never read. Skip the library
        # check rather than failing a run over inputs it will not use.
        if [ "~{search_qc}" = "true" ]; then
            echo "Validation passed (QC mode)."
            exit 0
        fi
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

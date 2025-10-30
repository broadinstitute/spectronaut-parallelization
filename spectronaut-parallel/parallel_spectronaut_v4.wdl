version development

workflow parallel_spectronaut {
    input {
        File fasta_1
        String file_directory
        String experiment_name
        File? fasta_2
        File? fasta_3
        File? analysis_schema
        File? enzyme_database
        File? report_schema_1
        File? report_schema_2
        File? report_schema_3
        File? report_schema_4
        File? json_settings

        # Compute resource configurations
        Int archive_generation_disk_gb = 2000
        Int search_disk_size_gb = 2000
        Int sne_combine_disk_gb = 2000
    }

    call list_files { input:
        gcs_path = file_directory,
    }

    Array[File] file_paths = read_lines(list_files.file_list)

    scatter (file_path in file_paths) {
        call archive_generation { input:
            input_file_path = file_path,
            fasta_1 = fasta_1,
            fasta_2 = fasta_2,
            fasta_3 = fasta_3,
            enzyme_database = enzyme_database,
            disk_gb = archive_generation_disk_gb,
        }
    }

    call combine_archives { input:
        input_archives = archive_generation.search_archive_psar,
    }

    scatter (file_path in file_paths) {
        call dia_analysis { input:
            input_file = file_path,
            search_archive = combine_archives.merged_archive,
            experiment_name = experiment_name,
            analysis_schema = analysis_schema,
            fasta_1 = fasta_1,
            fasta_2 = fasta_2,
            fasta_3 = fasta_3,
            json_settings = json_settings,

            disk_gb = search_disk_size_gb,
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

task archive_generation {
    input {
        String input_file_path
        File? fasta_1
        File? fasta_2
        File? fasta_3
        File? enzyme_database
        Int disk_gb
    }

    command <<<
        set -euo pipefail

        echo "=== Spectronaut Search Archive Generation ===" >&2
        echo "Input file: ~{input_file_path}" >&2

        input_dir=$(mktemp -d input_XXXXXX)
        output_dir=$(mktemp -d output_XXXXXX)

        gcloud storage cp -r "~{input_file_path}" "${input_dir}/"

        if [ ~{defined(enzyme_database)} = true ]; then
            echo "Importing enzyme database..." >&2
            dotnet SpectronautCMD.dll --importEnzymeDB "~{enzyme_database}"
        fi

        echo "Generating search archive..." >&2

        spectronaut lg -se Pulsar \
            -d "${input_dir}" \
            -fasta "~{fasta_1}" \
            ~{if defined(fasta_2) then "-fasta " + fasta_2 else ""} \
            ~{if defined(fasta_3) then "-fasta " + fasta_3 else ""} \
            -o "${output_dir}" 2>&1 | tee archive_generation.log

        kit_file=$(find "${output_dir}" -type f -name "*.kit" -print -quit)

        echo "Found search archive: ${kit_file}" >&2

        # Debug: List all files in output directory
        echo "Contents of output directory:" >&2
        ls -lah "${output_dir}" || echo "Output directory is empty or doesn't exist" >&2

        # Find the search archive files
        kit_file=$(find "${output_dir}" -type f -name "*.kit" -print -quit)

        if [ -z "${kit_file}" ]; then
            echo "ERROR: No .kit file found in output directory" >&2
            echo "Searching entire execution root for .kit files..." >&2
            find "${cromwell_root}" -type f -name "*.kit" >&2 || echo "No .kit files found anywhere" >&2
            exit 1
        fi

        echo "Found search archive: ${kit_file}" >&2

        psar_file=$(find "${output_dir}" -type f -name "*.psar" -print -quit)

        if [ -z "${psar_file}" ]; then
            echo "ERROR: No .psar file found in output directory" >&2
            echo "Searching entire execution root for .psar files..." >&2
            find "${cromwell_root}" -type f -name "*.psar" >&2 || echo "No .psar files found anywhere" >&2
            exit 1
        fi

        echo "Found search archive: ${psar_file}" >&2

        # Move to execution root with fixed filename for WDL output
        mv "${kit_file}" "${cromwell_root}/search_archive.kit"
        echo "Moved to execution root: ${cromwell_root}/search_archive.kit" >&2

        # Verify the file exists in the expected location
        if [ ! -f "${cromwell_root}/search_archive.kit" ]; then
            echo "ERROR: Failed to move file to execution root" >&2
            exit 1
        fi

        mv "${psar_file}" "${cromwell_root}/search_archive.psar"
        echo "Moved to execution root: ${cromwell_root}/search_archive.psar" >&2

        if [ ! -f "${cromwell_root}/search_archive.psar" ]; then
            echo "ERROR: Failed to move file to execution root" >&2
            exit 1
        fi

        echo "Archive generation completed successfully" >&2
        ls -lh "${cromwell_root}/search_archive.kit" >&2
        ls -lh "${cromwell_root}/search_archive.psar" >&2
    >>>

    output {
        File search_archive_kit = "search_archive.kit"
        File search_archive_psar = "search_archive.psar"
    }

    runtime {
        docker: "cameronlian/panoply-spectronaut:v20.0"
        cpu: 96
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

        # Work directly in execution root to avoid file path issues
        cromwell_root=$(pwd)
        echo "Cromwell execution root: ${cromwell_root}" >&2

        merged_library="merged_library.kit"

        # Create temporary directory for input archives
        archives_dir="${cromwell_root}/input_archives"
        mkdir -p "${archives_dir}"
        echo "Archives directory: ${archives_dir}" >&2

        # Copy all input archives to the temporary directory
        echo "Copying input archives..." >&2
        while IFS= read -r archive; do
            if [ -n "${archive}" ]; then
                cp "${archive}" "${archives_dir}/"
                echo "  Copied: $(basename "${archive}")" >&2
            fi
        done < ~{write_lines(input_archives)}

        # Verify archives were copied
        archive_count=$(find "${archives_dir}" -type f -name "*.psar" | wc -l)
        echo "Copied ${archive_count} archive(s) to ${archives_dir}" >&2

        if [ "${archive_count}" -eq 0 ]; then
            echo "ERROR: No archive files found in ${archives_dir}" >&2
            exit 1
        fi

        # List the archives
        echo "Archives to merge:" >&2
        ls -lh "${archives_dir}" >&2

        echo "Merging search archives..." >&2
        echo "Output file will be: ${cromwell_root}/${merged_library}" >&2

        spectronaut lg -se Pulsar \
            -sad "${archives_dir}" \
            -k "${cromwell_root}/${merged_library}" \
            -o "${cromwell_root}" 2>&1 | tee merge_archives.log

        echo "Spectronaut merge command completed. Verifying output..." >&2

        # Verify the merged library file was created in execution root
        if [ ! -f "${cromwell_root}/${merged_library}" ]; then
            echo "ERROR: Merged archive file not found: ${cromwell_root}/${merged_library}" >&2
            echo "Listing cromwell_root contents:" >&2
            ls -lah "${cromwell_root}" >&2
            exit 1
        fi

        echo "Archive merge completed successfully" >&2
        ls -lh "${cromwell_root}/${merged_library}" >&2
    >>>

    output {
        File merged_archive = "merged_library.kit"
    }

    runtime {
        docker: "cameronlian/panoply-spectronaut:v20.0"
        cpu: 32
        memory: "512GB"  # Memory intensive - 896GB is the max allowed on N2D VMs
        bootDiskSizeGb: 128
        disks: "local-disk 2000 SSD"
        preemptible: 0
    }
}

task dia_analysis {
    input {
        File input_file
        File search_archive
        File fasta_1
        String experiment_name
        File? analysis_schema
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

        # Work directly in execution root to avoid file path issues
        cromwell_root=$(pwd)
        echo "Cromwell execution root: ${cromwell_root}" >&2

        # Create output and temp directories in execution root (not tmp)
        output_dir="${cromwell_root}/dia_output"
        mkdir -p "${output_dir}"
        echo "Output directory: ${output_dir}" >&2

        tmp_dir="${cromwell_root}/dia_temp"
        mkdir -p "${tmp_dir}"
        echo "Temp directory: ${tmp_dir}" >&2

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

        # Find the SNE file
        sne_file=$(find "${output_dir}" -type f -name "*.sne" -print -quit)

        if [ -z "${sne_file}" ]; then
            echo "ERROR: No .sne file found in output directory" >&2
            exit 1
        fi

        echo "Found SNE file: ${sne_file}" >&2

        # Move to execution root with fixed filename for WDL output
        mv "${sne_file}" "${cromwell_root}/analysis.sne"

        echo "Moved to execution root: ${cromwell_root}/analysis.sne" >&2

        # Verify the file exists in the expected location
        if [ ! -f "${cromwell_root}/analysis.sne" ]; then
            echo "ERROR: Failed to move file to execution root" >&2
            exit 1
        fi

        echo "DIA analysis completed successfully" >&2
        ls -lh "${cromwell_root}/analysis.sne" >&2
    >>>

    output {
        File sne_file = "analysis.sne"
    }

    runtime {
        docker: "cameronlian/panoply-spectronaut:v20.0"
        cpu: 96  # CPU intensive
        memory: "256GB"
        bootDiskSizeGb: 128
        disks: "local-disk ~{disk_gb} SSD"
        preemptible: 0
    }
}

task combine_sne {
    input {
        File fasta_1
        Array[File] input_snes
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

        echo "=== SNE Combine ===" >&2
        echo "Experiment name: ~{experiment_name}" >&2
        echo "Number of input SNE files: ~{length(input_snes)}" >&2

        # Work directly in execution root
        cromwell_root=$(pwd)
        echo "Cromwell execution root: ${cromwell_root}" >&2

        output_dir="${cromwell_root}/sne_combine_output"
        output_zip="${cromwell_root}/spectronaut_output.zip"

        mkdir -p "${output_dir}"
        echo "Output directory: ${output_dir}" >&2

        # Create temporary directory for input SNE files
        sne_dir="${cromwell_root}/input_snes"
        mkdir -p "${sne_dir}"
        echo "SNE directory: ${sne_dir}" >&2

        # Copy all input SNE files to the temporary directory
        echo "Copying input SNE files..." >&2
        while IFS= read -r sne; do
            if [ -n "${sne}" ]; then
                cp "${sne}" "${sne_dir}/"
                echo "  Copied: $(basename "${sne}")" >&2
            fi
        done < ~{write_lines(input_snes)}

        # Verify SNE files were copied
        sne_count=$(find "${sne_dir}" -type f -name "*.sne" | wc -l)
        echo "Copied ${sne_count} SNE file(s) to ${sne_dir}" >&2

        if [ "${sne_count}" -eq 0 ]; then
            echo "ERROR: No SNE files found in ${sne_dir}" >&2
            exit 1
        fi

        # List the SNE files
        echo "SNE files to combine:" >&2
        ls -lh "${sne_dir}" >&2

        echo "Combining SNE files..." >&2
        spectronaut combine \
            -n "~{experiment_name}" \
            -o "${output_dir}" \
            -d "${sne_dir}" \
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

                # Verify the zip file was created
                if [ ! -f "${output_zip}" ]; then
                    echo "ERROR: Failed to create output zip file: ${output_zip}" >&2
                    exit 1
                fi

                echo "Output zip created successfully: ${output_zip}" >&2
                echo "SNE combine completed successfully" >&2
                ls -lh "${output_zip}" >&2
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

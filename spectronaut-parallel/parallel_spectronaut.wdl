version development

workflow spectronaut_htrms_conversion {
    input {
        String file_directory
        File? convert_schema
        String experiment_name

        Boolean do_conversion = true
        Boolean do_search = true

        File fasta_1
        File? fasta_2
        File? fasta_3
        File? analysis_settings
        File? enzyme_database
        File? spectral_library_1
        File? spectral_library_2
        File? report_schema_1
        File? report_schema_2
        File? report_schema_3
        File? report_schema_4
        File? json_settings

        File? htrms_manifest_input

        Int search_vm_count = 3
        Int search_cpu = 32
        Int search_memory_gb = 128
        Int search_disk_gb = 2000
        Int sneCombine_disk_gb = 2000
    }

    # Create a manifest of all input files in the input directory
    call list_files {
        input:
            gcs_path = file_directory
    }

    # Make an array of individual files from the generated file manifest
    Array[String] object_paths = read_lines(list_files.file_list)

    # htrms_manifest_input takes highest priority 
    # If an HTRMS manifest is provided, conversion will always be skipped even if do_conversion=true 
    Boolean skip_conversion = defined(htrms_manifest_input) && do_search

    if (do_conversion && !skip_conversion) {
        # Upon request, convert files to HTRMS format in parallel
        scatter (object_path in object_paths) {
            call htrms_conversion {
                input:
                    object_gcs_path = object_path,
                    convert_schema = convert_schema
            }
        }
        
        # Converted files are stored within their own VM output folder
        # Generate a listing of all converted HTRMS files
        # However, files are not copied into a single location to save time and space --> If needed, use `gcloud storage cp -r <parent_htrms_folder>/**.htrms <destination>` to copy all files into a single directory
        call gather_htrms {
            input:
                htrms_files = htrms_conversion.htrms_file
        }
    }

    # When no conversion is requested, user has the option to (A) supply their file directory containing either converted/unconverted files, or (B) supply a manifest of HTRMS files as the list generated at `gather_htrms`
    # If neither do_conversion nor htrms_manifest_input is supplied, generate a manifest from the input directory 
    if (!do_conversion && !defined(htrms_manifest_input)) {
        call manifest_from_directory {
            input:
                file_list = list_files.file_list
        }
    }

    # If htrms_manifest_input is supplied, the workflow will prioritize that manifest over generating another manifest from the supplied file directory 
    File? prioritized_manual_manifest = if (defined(htrms_manifest_input)) then htrms_manifest_input else None
    File resolved_htrms_manifest = select_first([
        prioritized_manual_manifest,
        gather_htrms.htrms_manifest,
        manifest_from_directory.htrms_manifest,
        htrms_manifest_input
    ])

    if (do_search) {
        # Partition files into search_vm_count batches for parallel searching in multiple VMs
        call partition_htrms_manifest {
            input:
                manifest = resolved_htrms_manifest,
                vm_count = search_vm_count
        }

        Array[File] search_batches = partition_htrms_manifest.batch_manifests
        Int search_shard_total = length(search_batches)
        Array[Int] search_indices = range(search_shard_total) # 

        # No report generated from individual search shards 
        scatter (shard in zip(search_indices, search_batches)) {
            call spectronaut_search_vm {
                input:
                    htrms_list = shard.right,
                    shard_index = shard.left,
                    shard_total = search_shard_total,
                    experiment_name = experiment_name,
                    analysis_settings = analysis_settings,
                    fasta_1 = fasta_1,
                    fasta_2 = fasta_2,
                    fasta_3 = fasta_3,
                    enzyme_database = enzyme_database,
                    spectral_library_1 = spectral_library_1,
                    spectral_library_2 = spectral_library_2,
                    json_settings = json_settings,
                    cpu = search_cpu,
                    memory_gb = search_memory_gb,
                    disk_gb = search_disk_gb
            }
        }

        # Same thing as done for HTRMS conversion, gather all generated SNE files into a manifest
        call collect_sne_manifest {
            input:
                sne_files = spectronaut_search_vm.sne_file
        }

        # Combine SNE files from scattered searches and generate reports
        # Note: The combined SNE file will not have a condition setup file associated --> user may need to manually attach one in the Spectronaut GUI if needed
        call combine_sne {
            input:
                sne_files = spectronaut_search_vm.sne_file,
                experiment_name = experiment_name,
                analysis_settings = analysis_settings,
                fasta_1 = fasta_1,
                fasta_2 = fasta_2,
                fasta_3 = fasta_3,
                report_schema_1 = report_schema_1,
                report_schema_2 = report_schema_2, 
                report_schema_3 = report_schema_3,
                report_schema_4 = report_schema_4,
                disk_gb = sneCombine_disk_gb
        }
    }

    output {
        File discovered_inputs = list_files.file_list
        Array[File]? converted_htrms = htrms_conversion.htrms_file
        File htrms_manifest = resolved_htrms_manifest
        Array[File]? vm_sne_files = spectronaut_search_vm.sne_file
        Array[Directory]? vm_search_outputs = spectronaut_search_vm.search_output_directory
        File? sne_manifest = collect_sne_manifest.sne_manifest
        Directory? combined_sne_output = combine_sne.combined_output
        Array[File]? combined_sne_files = combine_sne.combined_sne_files
        File? combined_sne_archive = combine_sne.combined_archive
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
        String object_gcs_path
        File? convert_schema
    }

    command <<<
        set -euo pipefail

        echo "=== Spectronaut HTRMS conversion ===" >&2
        echo "Source object: ~{object_gcs_path}" >&2

        tmp_root=$(mktemp -d tmp_input_XXXXXX)
        output_dir=$(mktemp -d tmp_output_XXXXXX)

        echo "Temporary input directory: ${tmp_root}" >&2
        echo "Temporary output directory: ${output_dir}" >&2

        # For timsTOF files, the entire .d directory needs to be copied recursively, and its parent directory will be used as input to the conversion
        # Otherwise risking not finding the input file to convert when the input file is a directory 
        echo "Copying source object into workspace..." >&2
        if ! gcloud storage cp -r "~{object_gcs_path}" "${tmp_root}/"; then
            echo "ERROR: Failed to copy object: ~{object_gcs_path}" >&2
            rm -rf "${tmp_root}" "${output_dir}"
            exit 1
        fi

        # Validate if the input file has been copied to the temporary directory 
        echo "Input directory contents:" >&2
        ls -lhR "${tmp_root}" >&2

        # The output should have the same base name as the input, but with .htrms extension 
        input_basename=$(basename "~{object_gcs_path}")
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
        File htrms_file = sub(basename(object_gcs_path), "\\.(raw|d|RAW|D)$", "") + ".htrms"
    }

    runtime {
        docker: "cameronlian/panoply-spectronaut:v20.0"
        cpu: 16
        memory: "32GB"
        bootDiskSizeGb: 128
        disks: "local-disk 512 SSD"
        preemptible: 0
    }
}

task gather_htrms {
    input {
        Array[File] htrms_files
    }

    command <<<
        set -euo pipefail

        manifest="htrms_files.txt"

        # Write files to temporary location first
        cat <<'EOF' > "${manifest}.tmp"
~{sep("\n", htrms_files)}
EOF

        echo "Converting file paths to GCS URLs..." >&2

        # Convert local Cromwell paths to GCS paths
        converted=0
        while IFS= read -r file_path; do
            if [ -z "${file_path}" ]; then
                continue
            fi
            if [[ "${file_path}" == /mnt/disks/cromwell_root/* ]]; then
                # Convert local Cromwell path to GCS path
                # Pattern: /mnt/disks/cromwell_root/bucket-name/... -> gs://bucket-name/...
                gcs_path="gs://${file_path#/mnt/disks/cromwell_root/}"
                echo "${gcs_path}"
                converted=$((converted + 1))
                echo "  Converted: ${file_path} -> ${gcs_path}" >&2
            elif [[ "${file_path}" == gs://* ]]; then
                # Already a GCS path
                echo "${file_path}"
                echo "  Already GCS path: ${file_path}" >&2
            else
                # Other format - pass through but warn
                echo "${file_path}"
                echo "  WARNING: Unrecognized path format: ${file_path}" >&2
            fi
        done < "${manifest}.tmp" > "${manifest}"

        rm "${manifest}.tmp"

        echo "Converted ${converted} local paths to GCS URLs" >&2
        echo "HTRMS manifest contents:" >&2
        cat "${manifest}" >&2

        # Validate that manifest contains valid entries
        line_count=$(grep -c "^gs://" "${manifest}" || true)
        if [ "${line_count}" -eq 0 ]; then
            echo "ERROR: Manifest does not contain any GCS paths" >&2
            exit 1
        fi

        echo "Manifest written to ${manifest} with ${line_count} GCS paths" >&2
    >>>

    output {
        File htrms_manifest = "htrms_files.txt"
    }

    runtime {
        docker: "google/cloud-sdk:slim"
        cpu: 8
        memory: "16GB"
        bootDiskSizeGb: 64
        disks: "local-disk 300 SSD"
    }
}

task manifest_from_directory {
    input {
        File file_list
    }

    command <<<
        set -euo pipefail

        python3 - <<'PY'
from pathlib import Path
import sys

file_list_path = Path("~{file_list}")

if not file_list_path.is_file():
    print(f"ERROR: file list not found: {file_list_path}", file=sys.stderr)
    sys.exit(1)

entries = []
with file_list_path.open("r", encoding="utf-8") as handle:
    for raw_line in handle:
        line = raw_line.strip()
        if not line:
            continue
        if line.endswith("/"):
            continue
        lower = line.lower()
        if lower.endswith(".htrms") or lower.endswith(".raw") or lower.endswith(".d"):
            entries.append(line)

if not entries:
    print("ERROR: No .htrms, .raw, or .d entries discovered in directory listing", file=sys.stderr)
    with file_list_path.open("r", encoding="utf-8") as handle:
        preview = handle.read()
    print("Listing contents for debugging:", file=sys.stderr)
    print(preview, file=sys.stderr)
    sys.exit(1)

manifest_path = Path("directory_htrms.txt")
manifest_path.write_text("\n".join(entries) + "\n", encoding="utf-8")

print(f"Wrote directory manifest with {len(entries)} entries to {manifest_path}", file=sys.stderr)
PY
    >>>

    output {
        File htrms_manifest = "directory_htrms.txt"
    }

    runtime {
        docker: "google/cloud-sdk:slim"
        cpu: 8
        memory: "16GB"
        bootDiskSizeGb: 64
        disks: "local-disk 300 SSD"
    }
}

task partition_htrms_manifest {
    input {
        File manifest
        Int vm_count
    }

    command <<<
        set -euo pipefail

        python3 - <<'PY'
import json
import math
import sys
from pathlib import Path

manifest_path = Path("~{manifest}")
vm_count = int("~{vm_count}")

if vm_count < 1:
    print(f"ERROR: vm_count must be >= 1 (got {vm_count})", file=sys.stderr)
    sys.exit(1)

if not manifest_path.is_file():
    print(f"ERROR: Manifest not found: {manifest_path}", file=sys.stderr)
    sys.exit(1)

entries = [line.strip() for line in manifest_path.read_text(encoding="utf-8").splitlines() if line.strip()]

if not entries:
    print("ERROR: Manifest did not contain any HTRMS files", file=sys.stderr)
    sys.exit(1)

batch_count = min(vm_count, len(entries))
batches = [[] for _ in range(batch_count)]

for idx, path in enumerate(entries):
    batches[idx % batch_count].append(path)

partitions_dir = Path("partitions")
partitions_dir.mkdir(parents=True, exist_ok=True)

for i, batch in enumerate(batches, start=1):
    target = partitions_dir / f"batch_{i:03d}.txt"
    target.write_text("\n".join(batch) + ("\n" if batch else ""), encoding="utf-8")

summary = partitions_dir / "summary.json"
summary.write_text(
    json.dumps(
        {
            "total_files": len(entries),
            "vm_requested": vm_count,
            "vm_created": batch_count,
            "distribution": [len(batch) for batch in batches],
        },
        indent=2,
    ),
    encoding="utf-8",
)

print(f"Created {batch_count} batch manifests from {len(entries)} inputs", file=sys.stderr)
for i, batch in enumerate(batches, start=1):
    print(f"  batch_{i:03d}.txt -> {len(batch)} files", file=sys.stderr)
PY
    >>>

    output {
        Array[File] batch_manifests = glob("partitions/batch_*.txt")
        File partition_summary = "partitions/summary.json"
    }

    runtime {
        docker: "google/cloud-sdk:slim"
        cpu: 8
        memory: "16GB"
        bootDiskSizeGb: 64
        disks: "local-disk 300 SSD"
    }
}

task spectronaut_search_vm {
    input {
        File htrms_list # Generated in partition_htrms_manifest
        Int shard_index # GGenerated in partition_htrms_manifest - scatter index
        Int shard_total 
        String experiment_name

        File fasta_1
        File? fasta_2
        File? fasta_3
        File? analysis_settings
        File? enzyme_database
        File? spectral_library_1
        File? spectral_library_2
        File? json_settings

        Int cpu
        Int memory_gb
        Int disk_gb
    }

    command <<<
        set -euo pipefail

        echo "=== Spectronaut search shard ===" >&2
        echo "Manifest: ~{htrms_list}" >&2
        echo "Shard index: ~{shard_index} of ~{shard_total}" >&2
        echo "Experiment name: ~{experiment_name}" >&2

        # Validate if the file pointed to by the manifest exists 
        if [ ! -f ~{htrms_list} ]; then
            echo "ERROR: HTRMS list file not found: ~{htrms_list}" >&2
            exit 1
        fi

        # Extract all lines from the htrms_manifest into an array of files
        mapfile -t htrms_files < ~{htrms_list}
        if [ ${#htrms_files[@]} -eq 0 ]; then
            echo "ERROR: No HTRMS files assigned to this shard" >&2
            exit 1
        fi

        # Builds a batch-specific experiment name suffix to avoid duplicate .sne names from multiple VMs
        batch_name=$(basename "~{htrms_list}") # takes the shard manifest path
        batch_name="${batch_name%.txt}" # Strip off ".txt" suffix
        suffix="${batch_name#batch_}" # Remove "batch_" prefix 
        exp_name="~{experiment_name}_vm${suffix}" # Create a new experiment name with the suffix 

        echo "Shard label: ${suffix}" >&2
        echo "Shard experiment name: ${exp_name}" >&2

        data_dir="spectronaut_vm_data"
        output_dir="spectronaut_vm_output"
        temp_dir="spectronaut_vm_temp"

        mkdir -p "${data_dir}" "${output_dir}" "${temp_dir}"

        echo "Copying ${#htrms_files[@]} HTRMS files into ${data_dir}" >&2
        echo "Files to copy:" >&2
        printf '%s\n' "${htrms_files[@]}" >&2

        copy_success=0
        copy_failed=0

        for src in "${htrms_files[@]}"; do
            if [ -z "${src}" ]; then
                continue
            fi
            dest="${data_dir}/$(basename "${src}")"

            if [[ "${src}" == gs://* ]]; then
                echo "  - downloading from GCS: ${src}" >&2
                if ! gcloud storage cp "${src}" "${dest}" 2>&1 | tee -a gcs_download.log >&2; then
                    echo "ERROR: Failed to download HTRMS file from GCS" >&2
                    echo "  Source: ${src}" >&2
                    echo "  Destination: ${dest}" >&2
                    echo "  Check gcs_download.log for details" >&2
                    copy_failed=$((copy_failed + 1))
                    exit 1
                fi
                copy_success=$((copy_success + 1))
            else
                echo "  - copying from local: ${src}" >&2
                if [ ! -e "${src}" ]; then
                    echo "ERROR: Missing HTRMS source file" >&2
                    echo "  Expected path: ${src}" >&2
                    echo "  This path does not exist in the current task environment" >&2
                    echo "  NOTE: If this is a local Cromwell path, it should have been" >&2
                    echo "        converted to a GCS path in the manifest. Check the" >&2
                    echo "        gather_htrms task output." >&2
                    copy_failed=$((copy_failed + 1))
                    exit 1
                fi
                if ! cp --reflink=auto "${src}" "${dest}" 2>/dev/null; then
                    cp "${src}" "${dest}"
                fi
                copy_success=$((copy_success + 1))
            fi
        done

        echo "File copy summary: ${copy_success} succeeded, ${copy_failed} failed" >&2

        echo "Data directory contents:" >&2
        ls -lh "${data_dir}" >&2

        if [ ~{defined(enzyme_database)} = true ]; then
            echo "Importing enzyme database..." >&2
            dotnet /usr/lib/spectronaut/SpectronautCMD.dll --importEnzymeDB "~{enzyme_database}"
        fi

        echo "Running Spectronaut search for shard ${suffix}" >&2
        log_path="spectronaut_vm.log"
        : > "${log_path}"

        spectronaut \
            ~{if !defined(spectral_library_1) then "-direct" else ""} \
            ~{if defined(analysis_settings) then "-s " + analysis_settings else ""} \
            ~{if defined(spectral_library_1) then "-a " + spectral_library_1 else ""} \
            ~{if defined(spectral_library_2) then "-a " + spectral_library_2 else ""} \
            ~{if defined(json_settings) then "-j " + json_settings else ""} \
            -n "${exp_name}" \
            -o "${output_dir}" \
            -fasta "~{fasta_1}" \
            ~{if defined(fasta_2) then "-fasta " + fasta_2 else ""} \
            ~{if defined(fasta_3) then "-fasta " + fasta_3 else ""} \
            -d "${data_dir}" \
            -setTemp "${temp_dir}" 2>&1 | tee -a "${log_path}" >&2
            # No report is generated at this step 

        search_status=${PIPESTATUS[0]}
        echo "Spectronaut exit status: ${search_status}" >&2

        if [ "${search_status}" -ne 0 ]; then
            echo "ERROR: Spectronaut search failed for shard ${suffix}" >&2
            exit "${search_status}"
        fi

        final_sne="${output_dir}/${exp_name}.sne"
        if [ -f "${final_sne}" ]; then
            echo "Spectronaut produced expected SNE file: ${final_sne}" >&2
        else
            echo "Locating SNE file within ${output_dir}" >&2
            mapfile -t sne_candidates < <(find "${output_dir}" -type f -name "*.sne")

            if [ ${#sne_candidates[@]} -eq 0 ]; then
                echo "ERROR: No SNE file produced for shard ${suffix}" >&2
                echo "Contents of output directory:" >&2
                find "${output_dir}" -type f >&2
                exit 1
            fi

            sne_file="${sne_candidates[0]}"
            if [ ${#sne_candidates[@]} -gt 1 ]; then
                echo "WARNING: Multiple SNE files detected; selecting ${sne_file}" >&2
                printf '  candidate: %s\n' "${sne_candidates[@]}" >&2
            fi

            if [ "${sne_file}" != "${final_sne}" ]; then
                echo "Relocating ${sne_file} to ${final_sne}" >&2
                if ! mv "${sne_file}" "${final_sne}"; then
                    echo "ERROR: Failed to move SNE file into ${output_dir}" >&2
                    exit 1
                fi
            else
                echo "Spectronaut produced correctly named file at ${sne_file}" >&2
            fi
        fi

        # Verify the final file exists
        if [ ! -f "${final_sne}" ]; then
            echo "ERROR: Final SNE file does not exist: ${final_sne}" >&2
            echo "Output directory contents:" >&2
            ls -lhR "${output_dir}" >&2
            exit 1
        fi

        echo "Shard SNE: ${final_sne}" >&2
        ls -lh "${final_sne}" >&2

        # Write the actual SNE filename to a file for WDL to read
        # This ensures WDL uses the exact same filename the bash script created
        printf '%s\n' "${exp_name}.sne" > sne_filename.txt
        if [ ! -s sne_filename.txt ]; then
            echo "ERROR: sne_filename.txt is empty; expected ${exp_name}.sne" >&2
            exit 1
        fi
        echo "SNE filename written to sne_filename.txt: ${exp_name}.sne" >&2

        # Preserve log alongside shard outputs for delocalization
        cp "${log_path}" "${output_dir}/spectronaut_vm.log"

        echo "Cleaning up local data copy" >&2
        rm -rf "${data_dir}" "${temp_dir}"
    >>>

    output {
        File sne_file = "spectronaut_vm_output/" + read_string("sne_filename.txt")
        Directory search_output_directory = "spectronaut_vm_output"
    }

    runtime {
        docker: "cameronlian/panoply-spectronaut:v20.0"
        cpu: cpu
        memory: "~{memory_gb}GB"
        bootDiskSizeGb: 512
        disks: "local-disk ~{disk_gb} SSD"
        preemptible: 0
    }
}

task collect_sne_manifest {
    input {
        Array[File] sne_files
    }

    command <<<
        set -euo pipefail

        manifest="sne_manifest.txt"

        # Write files to temporary location first
        cat <<'EOF' > "${manifest}.tmp"
~{sep("\n", sne_files)}
EOF

        echo "Converting file paths to GCS URLs..." >&2

        # Convert local Cromwell paths to GCS paths
        converted=0
        while IFS= read -r file_path; do
            if [ -z "${file_path}" ]; then
                continue
            fi
            if [[ "${file_path}" == /mnt/disks/cromwell_root/* ]]; then
                # Convert local Cromwell path to GCS path
                gcs_path="gs://${file_path#/mnt/disks/cromwell_root/}"
                echo "${gcs_path}"
                converted=$((converted + 1))
                echo "  Converted: ${file_path} -> ${gcs_path}" >&2
            elif [[ "${file_path}" == gs://* ]]; then
                # Already a GCS path
                echo "${file_path}"
                echo "  Already GCS path: ${file_path}" >&2
            else
                # Other format - pass through but warn
                echo "${file_path}"
                echo "  WARNING: Unrecognized path format: ${file_path}" >&2
            fi
        done < "${manifest}.tmp" > "${manifest}"

        rm "${manifest}.tmp"

        echo "Converted ${converted} local paths to GCS URLs" >&2
        echo "SNE manifest contents:" >&2
        cat "${manifest}" >&2

        # Validate that manifest contains valid entries
        line_count=$(grep -c "^gs://" "${manifest}" || true)
        if [ "${line_count}" -eq 0 ]; then
            echo "ERROR: Manifest does not contain any GCS paths" >&2
            exit 1
        fi

        echo "Manifest written to ${manifest} with ${line_count} GCS paths" >&2
    >>>

    output {
        File sne_manifest = "sne_manifest.txt"
    }

    runtime {
        docker: "google/cloud-sdk:slim"
        cpu: 8
        memory: "16GB"
        bootDiskSizeGb: 64
        disks: "local-disk 300 SSD"
    }
}

task combine_sne {
    input {
        Array[File] sne_files
        String experiment_name
        File? analysis_settings

        File fasta_1
        File? fasta_2
        File? fasta_3
        File? report_schema_1
        File? report_schema_2
        File? report_schema_3
        File? report_schema_4
        Int disk_gb = 2000
    }

    command <<<
        set -euo pipefail

        echo "=== Combining SNE files ===" >&2
        echo "Experiment name: ~{experiment_name}" >&2
        echo "SNE inputs: ~{length(sne_files)}" >&2

        if [ ~{length(sne_files)} -eq 0 ]; then
            echo "ERROR: No SNE files provided for combination" >&2
            exit 1
        fi

        python3 - <<'PY'
import json
from pathlib import Path

sne_files = json.loads(r'''~{write_json(sne_files)}''')
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

        out_dir="combined_output"
        mkdir -p "${out_dir}"

        sne_args=""
        while IFS= read -r sne; do
            [ -z "${sne}" ] && continue
            sne_args="${sne_args} -sne \"${sne}\""
        done < sne_inputs.txt

        echo "Running Spectronaut combine..." >&2
        # shellcheck disable=SC2086
        eval spectronaut -combine \
            -n "~{experiment_name}" \
            -o "${out_dir}" \
            ~{if defined(analysis_settings) then "-s " + analysis_settings else ""} \
            ${sne_args} \
            -fasta "~{fasta_1}" \
            ~{if defined(fasta_2) then "-fasta " + fasta_2 else ""} \
            ~{if defined(fasta_3) then "-fasta " + fasta_3 else ""} \
            ~{if defined(report_schema_1) then "-rs " + report_schema_1 else ""} \
            ~{if defined(report_schema_2) then "-rs " + report_schema_2 else ""} \
            ~{if defined(report_schema_3) then "-rs " + report_schema_3 else ""} \
            ~{if defined(report_schema_4) then "-rs " + report_schema_4 else ""} 2>&1 | tee spectronaut_combine.log >&2

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

        archive="combined_output.zip"
        echo "Creating archive ${archive}..." >&2
        if command -v pigz &> /dev/null; then
            echo "pigz detected; using tar+pigz for compression" >&2
            tar -cf - "${out_dir}" | pigz -p 8 > "${archive}"
            status=${PIPESTATUS[1]}
        else
            echo "Using zip for compression" >&2
            zip -r "${archive}" "${out_dir}"
            status=$?
        fi

        if [ "${status}" -ne 0 ]; then
            echo "ERROR: Failed to create archive ${archive}" >&2
            exit "${status}"
        fi

        if [ ! -f "${archive}" ]; then
            echo "ERROR: Archive not found after creation: ${archive}" >&2
            exit 1
        fi

        echo "Archive created successfully:" >&2
        ls -lh "${archive}" >&2
    >>>

    output {
        Directory combined_output = "combined_output"
        Array[File] combined_sne_files = glob("combined_output/*.sne")
        File combine_log = "spectronaut_combine.log"
        File combined_archive = "combined_output.zip"
    }

    runtime {
        docker: "cameronlian/panoply-spectronaut:v20.0"
        cpu: 32
        memory: "128GB"
        bootDiskSizeGb: 512
        disks: "local-disk ~{disk_gb} SSD"
        preemptible: 0
    }
}

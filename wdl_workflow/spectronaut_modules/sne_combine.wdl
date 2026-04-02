version 1.0

workflow sne_combine {
    input {
        # Required
        String sne_directory
        String experiment_name
        File analysis_schema

        # Mode toggle
        Boolean produce_final_sne = true

        # Disk size multiplier applied to total .sne file size (default: 50)
        Float disk_size_multiplier = 50.0

        # Optional
        File? condition_setup
        File? normal_report_schema_1
        File? normal_report_schema_2
        File? normal_report_schema_3
        File? normal_report_schema_4
        File? enzyme_database
    }

    Int combine_sne_cpu = 28
    # RAM per GB of SNE data — manageSNE --merge mode (produce_final_sne=true)
    Float combine_sne_ram_per_gb_merge = 3.0
    # RAM per GB of SNE data — spectronaut combine mode (produce_final_sne=false)
    Float combine_sne_ram_per_gb_combine = 0.8

    call measure_sne_size {
        input:
            sne_directory = sne_directory,
    }

    # RAM: scale with total SNE size, select rate based on mode (floor 64 GB, cap 700 GB)
    Float combine_sne_ram_per_gb = if produce_final_sne then combine_sne_ram_per_gb_merge else combine_sne_ram_per_gb_combine
    Int combine_sne_ram_gb = if ceil(combine_sne_ram_per_gb * measure_sne_size.total_sne_size_gb) > 700
                             then 700
                             else if ceil(combine_sne_ram_per_gb * measure_sne_size.total_sne_size_gb) > 64
                                 then ceil(combine_sne_ram_per_gb * measure_sne_size.total_sne_size_gb)
                                 else 64

    Int allocated_disk_gb = if ceil(measure_sne_size.total_sne_size_gb * disk_size_multiplier) > 10000
        then 10000
        else if ceil(measure_sne_size.total_sne_size_gb * disk_size_multiplier) > 500
            then ceil(measure_sne_size.total_sne_size_gb * disk_size_multiplier)
            else 500

    call combine_sne {
        input:
            sne_directory = sne_directory,
            experiment_name = experiment_name,
            analysis_schema = analysis_schema,
            produce_final_sne = produce_final_sne,
            ram_gb = combine_sne_ram_gb,
            cpu = combine_sne_cpu,
            allocated_disk_gb = allocated_disk_gb,
            condition_setup = condition_setup,
            normal_report_schema_1 = normal_report_schema_1,
            normal_report_schema_2 = normal_report_schema_2,
            normal_report_schema_3 = normal_report_schema_3,
            normal_report_schema_4 = normal_report_schema_4,
            enzyme_database = enzyme_database,
    }

    output {
        File spectronaut_output = combine_sne.spectronaut_output
    }
}

task combine_sne {
    input {
        String sne_directory
        File analysis_schema
        String experiment_name
        Boolean produce_final_sne
        Int ram_gb
        Int cpu
        Int allocated_disk_gb
        File? condition_setup
        File? normal_report_schema_1
        File? normal_report_schema_2
        File? normal_report_schema_3
        File? normal_report_schema_4
        File? enzyme_database
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

        mkdir -p "${output_dir}"

        sne_dir="${cromwell_root}/work_snes"
        mkdir -p "${sne_dir}"

        # Copy all .sne files (including from subfolders) into a flat directory.
        # The ** glob matches .sne files at any depth; omitting -r flattens them.
        echo "Copying SNE files from ~{sne_directory} (including subfolders)..."
        gcloud storage cp "~{sne_directory}/**.sne" "${sne_dir}/"

        sne_count=$(find "${sne_dir}" -maxdepth 1 -name "*.sne" | wc -l)
        echo "Found ${sne_count} .sne file(s)."
        if [ "${sne_count}" -eq 0 ]; then
            echo "ERROR: No .sne files found under ~{sne_directory}" >&2
            exit 1
        fi

        # Import enzyme database if provided
        if [ ~{defined(enzyme_database)} = true ]; then
            echo "Importing enzyme database..."
            dotnet SpectronautCMD.dll --importEnzymeDB "~{enzyme_database}"
        fi

        if [ "~{produce_final_sne}" = "true" ]; then
            spectronaut manageSNE --merge \
                -n "~{experiment_name}" \
                -o "${output_dir}" \
                -d "${sne_dir}" \
                ~{if defined(condition_setup) then "-con " + condition_setup else ""} \
                -s "~{analysis_schema}" \
                ~{if defined(normal_report_schema_1) then "-rs " + normal_report_schema_1 else ""} \
                ~{if defined(normal_report_schema_2) then "-rs " + normal_report_schema_2 else ""} \
                ~{if defined(normal_report_schema_3) then "-rs " + normal_report_schema_3 else ""} \
                ~{if defined(normal_report_schema_4) then "-rs " + normal_report_schema_4 else ""} 2>&1 | tee spectronaut_combine.log
        else
            spectronaut combine \
                -n "~{experiment_name}" \
                -o "${output_dir}" \
                -d "${sne_dir}" \
                ~{if defined(condition_setup) then "-con " + condition_setup else ""} \
                -s "~{analysis_schema}" \
                ~{if defined(normal_report_schema_1) then "-rs " + normal_report_schema_1 else ""} \
                ~{if defined(normal_report_schema_2) then "-rs " + normal_report_schema_2 else ""} \
                ~{if defined(normal_report_schema_3) then "-rs " + normal_report_schema_3 else ""} \
                ~{if defined(normal_report_schema_4) then "-rs " + normal_report_schema_4 else ""} 2>&1 | tee spectronaut_combine.log
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
        docker: "broadcptacdev/panoply_spectronaut:v20.5"
        cpu: cpu
        memory: "~{ram_gb}GB"
        bootDiskSizeGb: 32
        disks: "local-disk ~{allocated_disk_gb} HDD"
        preemptible: 0
    }
}

task measure_sne_size {
    input {
        String sne_directory
    }

    command <<<
        set -euo pipefail

        # Use gcloud storage ls -l with recursive glob to list all .sne files and their sizes
        # The ** glob matches .sne files at any depth (including subfolders)
        # The -l flag outputs: SIZE  DATE  gs://...
        total_bytes=$(gcloud storage ls -l "~{sne_directory}/**.sne" 2>/dev/null \
            | grep -v '^TOTAL:' \
            | awk '{sum += $1} END {print (sum == "" ? 0 : sum)}')

        if [ "${total_bytes}" -eq 0 ]; then
            echo "ERROR: No .sne files found under ~{sne_directory}" >&2
            exit 1
        fi

        total_gb=$(awk -v b="${total_bytes}" 'BEGIN { printf "%.4f", b / (1024^3) }')
        echo "Total .sne size: ${total_gb} GB (${total_bytes} bytes)"
        echo "${total_gb}" > total_sne_size_gb.txt
    >>>

    output {
        Float total_sne_size_gb = read_float("total_sne_size_gb.txt")
    }

    runtime {
        docker: "gcr.io/google.com/cloudsdktool/cloud-sdk:stable"
        cpu: 2
        memory: "8GB"
        preemptible: 2
        bootDiskSizeGb: 20
        disks: "local-disk 50 HDD"
    }
}

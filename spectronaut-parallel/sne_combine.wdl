version 1.0

workflow sne_combine {
    input {
        # Required
        String sne_gcs_path
        String experiment_name
        File analysis_schema
        Int n_data_files

        # experiment_type preset system
        String experiment_type = "proteome"   # "proteome" or "ptm"

        # Mode toggle
        Boolean produce_final_sne = true

        # Preemptible
        Int n_preemptible_combine_sne = 0

        # Optional
        File? condition_setup
        File? report_schema_1
        File? report_schema_2
        File? report_schema_3
        File? report_schema_4
        File? enzyme_database
    }

    Map[String, Int] combine_sne_cpu_presets = {
        "proteome": 32,
        "ptm": 32,
    }

    # RAM per data file (GB)
    Float ram_per_file_final   = 1.0   # produce_final_sne = true
    Float ram_per_file_combine = 0.5   # produce_final_sne = false

    # Disk per data file (GB)
    Float disk_per_file_final   = 5.0   # produce_final_sne = true
    Float disk_per_file_combine = 3.0   # produce_final_sne = false

    String validated_experiment_type = if (experiment_type == "proteome" || experiment_type == "ptm") then experiment_type else "proteome"

    Int combine_sne_cpu = combine_sne_cpu_presets[validated_experiment_type]

    Float ram_per_file  = if produce_final_sne then ram_per_file_final  else ram_per_file_combine
    Float disk_per_file = if produce_final_sne then disk_per_file_final else disk_per_file_combine

    Int combine_sne_ram_gb = ceil(ram_per_file  * n_data_files)
    Int allocated_disk_gb  = ceil(disk_per_file * n_data_files)

    call combine_sne {
        input:
            sne_gcs_path = sne_gcs_path,
            experiment_name = experiment_name,
            analysis_schema = analysis_schema,
            produce_final_sne = produce_final_sne,
            ram_gb = combine_sne_ram_gb,
            cpu = combine_sne_cpu,
            allocated_disk_gb = allocated_disk_gb,
            n_preemptible = n_preemptible_combine_sne,
            condition_setup = condition_setup,
            report_schema_1 = report_schema_1,
            report_schema_2 = report_schema_2,
            report_schema_3 = report_schema_3,
            report_schema_4 = report_schema_4,
            enzyme_database = enzyme_database,
    }

    output {
        File spectronaut_output = combine_sne.spectronaut_output
    }
}

task combine_sne {
    input {
        String sne_gcs_path
        File analysis_schema
        String experiment_name
        Boolean produce_final_sne
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

        echo "Copying SNE files recursively from ~{sne_gcs_path} ..."
        gcloud storage cp -r "~{sne_gcs_path}/**.sne" "${sne_dir}/"

        sne_count=$(find "${sne_dir}" -name "*.sne" | wc -l)
        echo "Found ${sne_count} .sne file(s)."
        if [ "${sne_count}" -eq 0 ]; then
            echo "ERROR: No .sne files found under ~{sne_gcs_path}" >&2
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
                ~{if defined(report_schema_1) then "-rs " + report_schema_1 else ""} \
                ~{if defined(report_schema_2) then "-rs " + report_schema_2 else ""} \
                ~{if defined(report_schema_3) then "-rs " + report_schema_3 else ""} \
                ~{if defined(report_schema_4) then "-rs " + report_schema_4 else ""} 2>&1 | tee spectronaut_combine.log
        else
            spectronaut combine \
                -n "~{experiment_name}" \
                -o "${output_dir}" \
                -d "${sne_dir}" \
                ~{if defined(condition_setup) then "-con " + condition_setup else ""} \
                -s "~{analysis_schema}" \
                ~{if defined(report_schema_1) then "-rs " + report_schema_1 else ""} \
                ~{if defined(report_schema_2) then "-rs " + report_schema_2 else ""} \
                ~{if defined(report_schema_3) then "-rs " + report_schema_3 else ""} \
                ~{if defined(report_schema_4) then "-rs " + report_schema_4 else ""} 2>&1 | tee spectronaut_combine.log
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
        docker: "broadcptacdev/panoply_spectronaut:v20.4"
        cpu: cpu
        memory: "~{ram_gb}GB"
        bootDiskSizeGb: 32
        disks: "local-disk ~{allocated_disk_gb} HDD"
        preemptible: n_preemptible
        cpuPlatform: "AMD Rome"
    }
}

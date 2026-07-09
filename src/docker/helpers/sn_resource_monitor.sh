#!/usr/bin/env bash
# Shared Spectronaut task resource monitor. Source, then call sn_monitor_start at the
# top of a command block and sn_monitor_report at the end. Output is intentionally
# byte-identical to the previously-inlined report so existing log parsing is unaffected.

sn_monitor_start() {
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
}

sn_monitor_report() {
    local allocated_cpus="$1"
    local allocated_disk_gb="$2"
    local root_dir="$3"

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

    echo "Disk Size Assigned: ${allocated_disk_gb} GB"

    if command -v df >/dev/null 2>&1; then
        disk_used_output=$(df -BG "${root_dir}" 2>/dev/null | tail -1 | awk '{print $3}')

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
}

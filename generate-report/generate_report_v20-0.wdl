version development

workflow panoply_spectronaut {
  input {
    File sne_file
    File report_schema_01
    File? report_schema_02
    File? report_schema_03
    File? report_schema_04
    File? report_schema_05
    Int ram_gb=256
  }

  call generate_report {
    input:
      sne_file=sne_file,
      report_schema_01=report_schema_01,
      report_schema_02=report_schema_02,
      report_schema_03=report_schema_03,
      report_schema_04=report_schema_04,
      report_schema_05=report_schema_05, 
      ram_gb=ram_gb
  }
}

task generate_report {
    input {
        File sne_file

        File report_schema_01
        File? report_schema_02
        File? report_schema_03
        File? report_schema_04
        File? report_schema_05

        Int ram_gb
    }

    command <<<
    set -euo pipefail

    echo "=== Generating reports using the provided report schemas ===" >&2

    mkdir -p reports_generated

    spectronaut manageSNE \
        -sne ~{sne_file} \
        -o reports_generated \
        -rs ~{report_schema_01} \
        ~{if defined(report_schema_02) then " -rs " + report_schema_02 else ""} \
        ~{if defined(report_schema_03) then " -rs " + report_schema_03 else ""} \
        ~{if defined(report_schema_04) then " -rs " + report_schema_04 else ""} \
        ~{if defined(report_schema_05) then " -rs " + report_schema_05 else ""}

    echo "=== Report generation complete ===" >&2
    >>>

    output {
        Directory reports_dir="reports_generated"
    }

    runtime {
      docker: "broadcptacdev/panoply_spectronaut:v20.0"
      cpuPlatform: "AMD Rome"
      memory: "~{ram_gb}GB"   # 896GB max for AMD Rome
      bootDiskSizeGb: 512
      disks : "local-disk 1000 HDD"
      preemptible : 0
      cpu: 16
    }
}
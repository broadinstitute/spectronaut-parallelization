version development

workflow sne_combine {
  input {
    String experiment_name

    File? settings_schema

    File? sne_01
    File? sne_02
    File? sne_03
    File? sne_04
    File? sne_05

    Directory? sne_folder_01
    Directory? sne_folder_02
    Directory? sne_folder_03

    File fasta_01
    File? fasta_02
    File? fasta_03

    File? report_schema_01
    File? report_schema_02
    File? report_schema_03

    Int num_cpus=32
    Int ram_gb=128
    Int local_disk_gb=2000
  }

  call do_combine {
    input:
      experiment_name=experiment_name,
      settings_schema=settings_schema,
      sne_01=sne_01,
      sne_02=sne_02,
      sne_03=sne_03,
      sne_04=sne_04,
      sne_05=sne_05,
      sne_folder_01=sne_folder_01,
      sne_folder_02=sne_folder_02,
      sne_folder_03=sne_folder_03,
      fasta_01=fasta_01,
      fasta_02=fasta_02,
      fasta_03=fasta_03,
      report_schema_01=report_schema_01,
      report_schema_02=report_schema_02,
      report_schema_03=report_schema_03,
      num_cpus=num_cpus,
      ram_gb=ram_gb,
      local_disk_gb=local_disk_gb
  }
}

task do_combine {
  input {
    String experiment_name

    File? settings_schema

    File? sne_01
    File? sne_02
    File? sne_03
    File? sne_04
    File? sne_05

    Directory? sne_folder_01
    Directory? sne_folder_02
    Directory? sne_folder_03

    File fasta_01
    File? fasta_02
    File? fasta_03

    File? report_schema_01
    File? report_schema_02
    File? report_schema_03

    Int num_cpus
    Int ram_gb
    Int local_disk_gb
  }

  command <<<
    set -euo pipefail

    echo "=== SNE combine task started (broadcptacdev/panoply_spectronaut:v20.0) ===" >&2
    echo "Experiment name: ~{experiment_name}" >&2

    out_zip="sne_combined_output.zip"
    out_dir="sne_combined"
    cromwell_root=$(pwd) # use cromwell_root fs for both wd and temp dir
    sn_temp=$(mktemp -d sn_temp_XXXXXX) # temp dir for Spectronaut -- else runs out of space on root fs
    working_dir=$(mktemp -d working_dir_XXXXXX) # use wd in the /cromwell_root file system
    cd "$working_dir"

    mkdir -p $out_dir

    echo "=== Running Spectronaut combine ===" >&2

    spectronaut -combine \
    -n ~{experiment_name} \
    ~{if defined(settings_schema) then " -s " + settings_schema else ""} \
    ~{if defined(sne_01) then " -sne " + sne_01 else ""} \
    ~{if defined(sne_02) then " -sne " + sne_02 else ""} \
    ~{if defined(sne_03) then " -sne " + sne_03 else ""} \
    ~{if defined(sne_04) then " -sne " + sne_04 else ""} \
    ~{if defined(sne_05) then " -sne " + sne_05 else ""} \
    ~{if defined(sne_folder_01) then " -d " + sne_folder_01 else ""} \
    ~{if defined(sne_folder_02) then " -d " + sne_folder_02 else ""} \
    ~{if defined(sne_folder_03) then " -d " + sne_folder_03 else ""} \
    -fasta ~{fasta_01} \
    ~{if defined(fasta_02) then " -fasta " + fasta_02 else ""} \
    ~{if defined(fasta_03) then " -fasta " + fasta_03 else ""} \
    ~{if defined(report_schema_01) then " -rs " + report_schema_01 else ""} \
    ~{if defined(report_schema_02) then " -rs " + report_schema_02 else ""} \
    ~{if defined(report_schema_03) then " -rs " + report_schema_03 else ""} \
    -o $out_dir \
    -setTemp "$sn_temp"

    echo "=== Packaging outputs ===" >&2
    zip -r $out_zip $out_dir -x \\*.zip
    mv "$out_zip" "$cromwell_root/"

    echo "=== SNE combine complete (broadcptacdev/panoply_spectronaut:v20.0) ===" >&2
  >>>

  output {
    File sne_combined_output="sne_combined_output.zip"
  }

  runtime {
    docker: "broadcptacdev/panoply_spectronaut:v20.0"
    cpuPlatform: "AMD Rome"
    memory: "~{ram_gb} GB"   # 896GB max for AMD Rome
    bootDiskSizeGb: 512
    disks : "local-disk ~{local_disk_gb} HDD"
    preemptible : 0
    cpu: num_cpus
  }

  meta {
    author: "C. Lian"
    email : "glian@broadinstitute.org"
  }
}
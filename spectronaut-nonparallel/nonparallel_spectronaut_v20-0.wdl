version development

workflow panoply_spectronaut {
    input {
        Directory files_folder
        String experiment_name
        Boolean do_conversion = true
        Boolean do_search = true
    }

    if (do_conversion) {
        call convert_htrms { input:
            files_folder = select_first([
                files_folder,
            ]),
        }
    }

    if (do_search) {
        call spectronaut { input:
            experiment_name = experiment_name,
            files_folder = select_first([
                convert_htrms.htrms_dir,
                files_folder,
            ]),
        }
    }
}

task convert_htrms {
    meta {
        author: "C. Lian"
        email: "glian@broadinstitute.org"
    }

    input {
        Directory files_folder  # raw folder path passed in because WDL 1.0 does not support Directory input type
        File? convert_schema
        Int local_disk_gb = 2000
    }

    command <<<
        set -euo pipefail

        echo "=== Converting HTRMS files using broadcptacdev/panoply_spectronaut:v20.0 ===" >&2

        mkdir -p htrms_converted

        spectronaut -convert \
        -i ~{files_folder} \
        -o htrms_converted \
        ~{if defined(convert_schema) then " -s " + convert_schema else ""}

        echo "=== Finished converting HTRMS files using broadcptacdev/panoply_spectronaut:v20.0 ===" >&2
    >>>

    output {
        Directory htrms_dir = "htrms_converted"
    }

    runtime {
        docker: "cameronlian/panoply-spectronaut:v20.3"
        cpuPlatform: "AMD Rome"
        memory: "64GB"
        bootDiskSizeGb: 512
        disks: "local-disk ~{local_disk_gb} HDD"
        preemptible: 0
        cpu: 16
    }
}

task spectronaut {
    meta {
        author: "D. R. Mani, C. Lian"
        email: "proteogenomics@broadinstitute.org"
    }

    input {

        # Search databases
        File fasta
        Directory files_folder
        String experiment_name
        File? analysis_settings
        File? condition_setup
        File? fasta_1
        File? enzyme_database

        # Spectral libraries - if none specified, perform DirectDIA
        File? spectral_library
        File? spectral_library_1

        # Report schema
        File? report_schema
        File? report_schema_1
        File? report_schema_2
        File? json_settings
        Int num_cpus = 16
        Int ram_gb = 64
        Int local_disk_gb = 2000
    }

    command <<<
        set -euo pipefail

        echo "=== Spectronaut analysis task started (broadcptacdev/panoply_spectronaut:v20.0) ===" >&2
        echo "directDIA mode: ~{if !defined(spectral_library) then "true" else "false"}" >&2

        out_zip="spectronaut_output.zip"
        out_dir="spectronaut/out"
        cromwell_root=$(pwd) # use cromwell_root fs for both wd and temp dir
        sn_temp=$(mktemp -d sn_temp_XXXXXX) # temp dir for Spectronaut -- else runs out of space on root fs
        working_dir=$(mktemp -d working_dir_XXXXXX) # use wd in the /cromwell_root file system
        cd $working_dir

        mkdir -p $out_dir
        tmp_dir=$(mktemp -d data_XXXXXX) # in case the files_folder is named 'data'
        mv ~{files_folder}/* $tmp_dir # all under $cromwell_root -- no need to copy
        mv $tmp_dir data

        echo "=== Running Spectronaut ===" >&2

        ~{if defined(enzyme_database) then "dotnet /usr/lib/spectronaut/SpectronautCMD.dll --importEnzymeDB "
            + enzyme_database else ""}

        spectronaut \
          ~{if !defined(spectral_library) then " -direct" else ""} \
          ~{if defined(analysis_settings) then " -s " + analysis_settings else ""} \
          ~{if defined(condition_setup) then " -con " + condition_setup else ""} \
          ~{if defined(fasta_1) then " -fasta " + fasta_1 else ""} \
          ~{if defined(spectral_library) then " -a " + spectral_library else ""} \
          ~{if defined(spectral_library_1) then " -a " + spectral_library_1 else ""} \
          ~{if defined(report_schema) then " -rs " + report_schema else ""} \
          ~{if defined(report_schema_1) then " -rs " + report_schema_1 else ""} \
          ~{if defined(report_schema_2) then " -rs " + report_schema_2 else ""} \
          ~{if defined(json_settings) then " -j " + json_settings else ""} \
          -n ~{experiment_name} \
          -o $out_dir \
          -fasta ~{fasta} \
          -d data \
          -setTemp $sn_temp

        zip -r $out_zip $out_dir -x \*.zip
        mv $out_zip /$cromwell_root/

        echo "=== Spectronaut search complete (broadcptacdev/panoply_spectronaut:v20.0) ===" >&2
    >>>

    output {
        File spectronaut_output = "spectronaut_output.zip"
    }

    runtime {
        docker: "cameronlian/panoply-spectronaut:v20.3"
        cpuPlatform: "AMD Rome"
        memory: "~{ram_gb}GB"  # 896GB max for AMD Rome
        bootDiskSizeGb: 512
        disks: "local-disk ~{local_disk_gb} HDD"
        preemptible: 0
        cpu: num_cpus
    }
}

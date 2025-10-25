version development

workflow panoply_spectronaut_parallel {
    input {
        Directory file_directory
        File? convert_schema 
    }

    call list_files {
        input: 
            file_directory = file_directory
    }
}

task list_files {
    input {
        Directory file_directory
    }

    command <<<
        set -euo pipefail

        gcloud storage ls -d "${file_directory}/*" 2>/dev/null > file_list_raw.txt || true

        # Clean up output: remove trailing slashes, blanks, and duplicates
        sed 's:/$::' file_list_raw.txt | grep -v '^$' | sort -u > file_list.txt
    >>>

    output {
        File file_list = "file_list.txt"
    }
}
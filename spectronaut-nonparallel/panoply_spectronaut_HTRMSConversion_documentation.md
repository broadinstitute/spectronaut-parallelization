# Terra-101: How to Run Spectronaut Searches on Terra?

## Convert Files to HTRMS before Your Search

Starting with snapshots v24 and v25, `panoply_spectronaut` supports converting raw files to HTRMS format before running the search. 

HTRMS Converter extracts and encodes MS data from the instrument into HTRMS, a format optimized for Spectronaut, enabling faster and more cost-efficient searches. If files are converted to HTRMS format, the converted files will be saved in the `call-htrms_convert` folder within the submission output directory (i.e., the same place where the search output is stored) and can be transferred to your bucket for future searches.

### New Input Variables for HTRMS Conversion

- `do_conversion` and `do_search` let you control whether to run conversion and/or Spectronaut search 
- If converting files, set `files_folder` to the directory containing raw files 
  - If you already have HTRMS files, set `htrms_files_folder` to that directory. When `htrms_files_folder` is defined, the workflow skips conversion and uses these converted files for the search, regardless of `do_conversion` or `files_folder` 
- `convert_scheme` specifies a custom HTRMS conversion scheme exported from HTRMS Converter (BGS Factory Default is used if not defined)
- If converting files, update the `Run Label` column in the `condition_setup` file so file extensions are ".htrms"  
- All other variables remain the same as in previous snapshot versions 



![HTRMS Conversion variable annotations](./assets/New panoply_spectronaut Workflow Annotation.png)



## gcloud CLI Cheat Sheet

To authenticate gcloud CLI, run `gcloud auth login`. 

*Once ANY user has authenticated on a machine, re-authentication is not required for new command line sessions in the future. 

### Transferring Files, BUT FASTER!

You do NOT need to run `gcloud config set project <project-id>` before transferring files. 

Once the CLI has been authenticated, files can be copied directly between different buckets with the following commands: 

- Copy a file to a destination: `gcloud storage cp <source> <destination>` 
- Copy multiple files to a destination: `gcloud storage cp <source-1> <source-2> ... <source-n> <destination>`
- Copy a folder to a destination: `gcloud storage cp --recursive <source> <destination>`

*The new `gcloud storage` command accelerates the transfer through parallelization.



## `panoply_spectronaut` Snapshot Info

==Terra does NOT track snapshot versions, so we recommend noting your snapshot version in job comment for future reference.==

| **Snapshot** | **Spectronaut** | **Creation Date** |                          **Notes**                           |
| :----------: | :-------------: | :---------------: | :----------------------------------------------------------: |
|      15      |      v18.7      |    2024-06-20     |                                                              |
|      16      |      v19.0      |    2024-06-20     |                                                              |
|      17      |      v19.7      |    2024-08-15     |                                                              |
|      18      |      v19.0      |    2024-10-17     |                                                              |
|      19      |      v19.7      |    2024-11-06     |               Default computational parameters               |
|      20      |      v20.0      |    2025-06-16     |               Default computational parameters               |
|      22      |      v20.0      |    2025-09-17     | Reduced computational parameters to 16 CPU cores, 64GB RAM, and 2TB disk space |
|      23      |      v19.7      |    2025-09-17     | Reduced computational parameters to 16 CPU cores, 64GB RAM, and 2TB disk space |
|      24      |      v20.0      |    2025-09-25     |                    Added HTRMS Conversion                    |
|      25      |      v19.7      |    2025-09-25     |                    Added HTRMS Conversion                    |

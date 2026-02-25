# parallel_spectronaut — Parallelized Spectronaut directDIA Workflow

> **For the Broad Institute Proteogenomics Team**
> Contact: proteogenomics@broadinstitute.org

---

## Table of Contents

1. [Introduction](#introduction)
2. [What's New Compared to the Original Workflow](#whats-new-compared-to-the-original-workflow)
3. [Getting Started: Input Variables](#getting-started-input-variables)
4. [Usage on Terra and Google Cloud Platform](#usage-on-terra-and-google-cloud-platform)
   - [Step 0: Prepare Your Input Files](#step-0-prepare-your-input-files)
   - [Step 1: Set Up gcloud CLI and Authenticate](#step-1-set-up-gcloud-cli-and-authenticate)
   - [Step 2: Upload Files to Your Terra Bucket](#step-2-upload-files-to-your-terra-bucket)
   - [Step 3: Add the Workflow to Your Terra Workspace](#step-3-add-the-workflow-to-your-terra-workspace)
   - [Step 4: Configure and Launch the Search](#step-4-configure-and-launch-the-search)
   - [Step 5: Collect Your Results](#step-5-collect-your-results)
5. [Troubleshooting](#troubleshooting)
6. [Contacts](#contacts)

---

## Introduction

As mass spectrometry experiments grow in scale — from dozens to hundreds or even thousands of samples — the time it takes to analyze the data becomes a real bottleneck. Running a Spectronaut directDIA search on 200 human proteome samples on a single computer can take several days, by which point more data may already be waiting.

This workflow — `parallel_spectronaut` — solves this problem by splitting your dataset across multiple cloud computers (called **virtual machines**, or VMs) that all work simultaneously. Instead of one computer processing all your files in sequence, many computers each handle a fraction of the files at the same time. The final results are then automatically merged into a single experiment-level output, exactly as if everything had run together.

**The result: a 200-sample human proteome experiment that previously took days can now complete in approximately 8 hours — at comparable or lower cost.**

### How the Workflow Works

The workflow runs through the following stages automatically:

```
Your raw MS files (in Google Cloud)
        │
        ▼
1. [List & Bin Files]  — Divides files evenly across your chosen number of VMs
        │
        ▼
2. [Optional: HTRMS Conversion]  — Converts raw instrument files to .htrms format (per VM, in parallel)
        │
        ▼
3. [Pulsar Step 1 — per VM]  — Each VM generates an intermediate search archive (.psar)
        │
        ▼
4. [Pulsar Step 2 — one VM]  — All intermediate archives are combined to train optimized models
        │
        ▼
5. [Pulsar Step 3 — per VM]  — Each VM generates a final search archive using the optimized models
        │
        ▼
6. [Combine Archives — one VM]  — All final archives are merged into a single search library (.kit)
        │
        ▼
7. [DIA Analysis — per VM]  — Each VM runs DIA analysis against the merged library, producing .sne files
        │
        ▼
8. [Combine SNE — one VM]  — All .sne files are merged and reports are generated
        │
        ▼
        spectronaut_output.zip  (your final results)
```

If you set `num_vms = 1`, the workflow instead runs a classic single-VM directDIA search — no parallelization, simpler and cheaper for small datasets.

### Saving Money with Preemptible Instances

One of the important features of this workflow is built-in support for **preemptible instances**.

**What is a preemptible instance?**
When Google Cloud runs a computation job, it can do so on two types of virtual machines:
- **Standard instances** — reserved just for your job, always available, but more expensive.
- **Preemptible instances** — run on spare computing capacity that Google has available at a given moment. They are **50–80% cheaper** than standard instances, but Google can "reclaim" them (i.e., shut them down) if that capacity is needed elsewhere. If that happens, your task simply retries automatically.

For long-running, embarrassingly parallel tasks like search archive generation — where losing one VM just means one bin retries — preemptible instances are an excellent way to reduce cost. The workflow is designed so that tasks most tolerant of interruption (Steps 1 and 3) default to using 1 preemptible attempt, while the non-interruptible combination steps default to non-preemptible.

You can control preemptibility per task using the `n_preemptible_*` input variables (see [Getting Started](#getting-started-input-variables) below).

### Automatic Disk and Resource Sizing

Figuring out how much disk space or memory to allocate for a cloud job can be confusing. Too little and the job crashes; too much and you're wasting money. This workflow handles this for you:

- **Disk space** is calculated automatically based on the actual size of your input data. The workflow measures your files in Google Cloud and multiplies by a configurable safety margin (`disk_size_multiplier`, default 3×). You do not need to guess.
- **CPU and RAM** are set automatically based on a single input: `experiment_type`. Set it to `"proteome"` for standard proteomics experiments, or `"ptm"` for post-translational modification experiments (which typically require more memory). The workflow selects appropriate resources for each step accordingly — no manual tuning needed for most cases.

---

## What's New Compared to the Original Workflow

The original single-VM workflow (`nonparallel_spectronaut`) processes all files on one machine, sequentially. Here is how `parallel_spectronaut` differs:

| Feature | Original Workflow | Parallel Workflow |
|---|---|---|
| **Processing model** | Single VM, all files in sequence | Multiple VMs, files split across them |
| **Runtime (200 samples)** | Several days | ~8 hours |
| **Preemptible instance support** | Not supported | Fully configurable per task |
| **Disk sizing** | Manual — you specify a fixed disk size | Automatic — calculated from actual input file sizes |
| **Resource configuration** | Manual — specify CPU and RAM individually | Simplified — set `experiment_type` to configure all resources |
| **Pulsar multi-step search** | No — single directDIA run | Yes — 3-step Pulsar search with cross-bin model training |
| **Resume on failure** | Limited — entire workflow may restart | Yes — completed tasks are cached; failed tasks resume from checkpoint |
| **Diagnostic logging** | Minimal | Resource usage report (CPU, RAM, disk) included per task |
| **Output** | `spectronaut_output.zip` | `spectronaut_output.zip` (same format) |

> **Data quality note:** QC testing with 7 Jurkat samples on an Astral instrument showed comparable PCA separations and FDR distributions between the two workflows — meaning parallelization does not compromise your results.

---

## Getting Started: Input Variables

Below is a complete list of all input variables, sorted alphabetically. Variables marked with ⭐ are the ones you will most commonly need to set.

### Required Inputs

| Variable | Type | Description |
|---|---|---|
| ⭐ `directDIA_settings` | File | Your Spectronaut directDIA settings file (`.prop`), exported from the Spectronaut GUI. This is used for all search steps. |
| ⭐ `experiment_name` | String | A name for your experiment. This will appear in the output file names. |
| ⭐ `experiment_type` | String | `"proteome"` or `"ptm"`. Sets CPU and RAM presets for all tasks automatically. Default: `"proteome"`. |
| ⭐ `fasta_1` | File | Your primary protein sequence database (FASTA format). Required for the search. |
| ⭐ `file_directory` | String | The Google Cloud Storage (GCS) path to the folder containing your raw input files (e.g., `gs://your-bucket/your-folder/`). |
| ⭐ `num_vms` | Integer | How many VMs to use. Set to `1` for a classic single-VM directDIA run. Set to `4`, `8`, `16`, etc. for parallel processing. Your files will be distributed evenly across the VMs. Maximum: 80. |

### Optional — Additional Databases & Settings

| Variable | Type | Description |
|---|---|---|
| `condition_setup` | File (optional) | A condition setup file (`.tsv`) that maps your run names to experimental conditions. |
| `enzyme_database` | File (optional) | A custom enzyme database file, if your experiment uses non-standard enzymes. |
| `fasta_2` | File (optional) | A second FASTA database. Spectronaut will combine it with `fasta_1` before searching. |
| `fasta_3` | File (optional) | A third FASTA database. |
| `json_settings` | File (optional) | A JSON settings file for Spectronaut. |

### Optional — HTRMS Conversion

| Variable | Type | Default | Description |
|---|---|---|---|
| `convert_schema` | File (optional) | — | A custom HTRMS conversion schema (`.prop`), exported from the HTRMS Converter GUI. If not provided, Biognosys default settings are used, which works for most cases. |
| `do_conversion` | Boolean | `false` | Set to `true` if your input files need to be converted to HTRMS format before searching. |

### Optional — Report Schemas

| Variable | Type | Description |
|---|---|---|
| `report_schema_1` | File (optional) | A Spectronaut report schema (`.rs`), exported from the GUI. Reports will be generated at the end of the workflow. |
| `report_schema_2` | File (optional) | Additional report schema (up to 4 total). |
| `report_schema_3` | File (optional) | Additional report schema. |
| `report_schema_4` | File (optional) | Additional report schema. |

### Optional — Disk Sizing

| Variable | Type | Default | Description |
|---|---|---|---|
| `average_file_size_gb` | Float | `20` | Estimated average size of each input file in GB. Used for an early disk size estimate before actual file sizes are measured. The default of 20 GB is suitable for typical raw Astral files. |
| `disk_size_multiplier` | Float | `3` | Safety multiplier applied to the total input data size when allocating disk space for most tasks. Increase this if you encounter "out of disk" errors. |
| `sne_combine_disk_size_multiplier` | Float | `6` | Separate disk multiplier for the SNE combine step, which may need more space. |

### Optional — Preemptible Instance Settings

Setting any of these to `0` means that task will use a standard (non-preemptible) instance. Setting to `1` or higher means the task will try that many times on a preemptible instance before falling back to a standard instance.

| Variable | Type | Default | Description |
|---|---|---|---|
| `n_preemptible_combine_archives` | Integer | `0` | Preemptible attempts for the archive combining step. |
| `n_preemptible_combine_sne` | Integer | `0` | Preemptible attempts for the SNE combining step. |
| `n_preemptible_dia_analysis` | Integer | `1` | Preemptible attempts for DIA analysis (per VM). |
| `n_preemptible_directDIA_single_vm` | Integer | `0` | Preemptible attempts for single-VM directDIA mode (`num_vms = 1`). |
| `n_preemptible_htrms_conversion` | Integer | `2` | Preemptible attempts for HTRMS conversion. |
| `n_preemptible_pulsar_step1` | Integer | `1` | Preemptible attempts for Pulsar Step 1 (intermediate archive generation). |
| `n_preemptible_pulsar_step2` | Integer | `0` | Preemptible attempts for Pulsar Step 2 (model training). |
| `n_preemptible_pulsar_step3` | Integer | `1` | Preemptible attempts for Pulsar Step 3 (final archive generation). |

---

## Usage on Terra and Google Cloud Platform

### Step 0: Prepare Your Input Files

Before launching the workflow, you need:

1. **Your raw MS data files** — in a Google Cloud Storage (GCS) bucket, all in the same folder.
2. **A FASTA database** — uploaded to your Terra workspace bucket or another accessible GCS bucket.
3. **A directDIA settings file** (`.prop`) — exported from Spectronaut's GUI on your local machine, then uploaded to GCS.
4. *(Optional)* Report schemas, condition setup file, convert schema — exported from Spectronaut's GUI, then uploaded to GCS.

**How to export settings files from Spectronaut GUI:**
- For directDIA settings: In Spectronaut, go to *Config → Export Settings*, save as `.prop`.
- For report schemas: In Spectronaut, go to *Reports → Export Schema*, save as `.rs`.
- For HTRMS conversion schema: In HTRMS Converter, go to *Settings → Export*, save as `.prop`.

---

### Step 1: Set Up gcloud CLI and Authenticate

The **gcloud CLI** (command-line interface) is a tool that lets you transfer files between your computer and Google Cloud Storage. You will need it to upload your input files to your Terra workspace bucket.

#### Installation

**On macOS or Linux:**
```bash
# Download and run the installer
curl https://sdk.cloud.google.com | bash

# Restart your shell, then initialize
gcloud init
```

**On Windows (PowerShell):**
```powershell
# Download the installer
(New-Object Net.WebClient).DownloadFile("https://dl.google.com/dl/cloudsdk/channels/rapid/GoogleCloudSDKInstaller.exe", "$env:Temp\GoogleCloudSDKInstaller.exe")

# Run the installer
& $env:Temp\GoogleCloudSDKInstaller.exe
```
After installation completes, open a new Command Prompt and run:
```powershell
gcloud init
```

#### Authenticate with Your Broad Google Account

```bash
gcloud auth login
```

This will open a browser window asking you to sign in with your Google account. **Use your Broad Institute Google account** (the one that has access to your Terra workspace).

> **Troubleshooting: "You do not have access to this bucket"**
>
> If you see a permission error when trying to copy files to a GCS bucket, it most likely means that a different person previously authenticated on this computer using their credentials, and their account does not have access to your private Terra workspace bucket. To fix this, simply re-authenticate with your own account:
> ```bash
> gcloud auth login
> ```
> Sign in with your Broad account when prompted, and then retry the file transfer.

---

### Step 2: Upload Files to Your Terra Bucket

Each Terra workspace has a dedicated Google Cloud Storage bucket. You can find the bucket path by:
1. Going to your workspace on Terra (app.terra.bio).
2. Clicking the **"Google Bucket"** icon or link near the top right of the workspace page.
3. The path will look like: `gs://fc-xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx/`

#### Upload a single file

```bash
gcloud storage cp /path/to/your/local/file.prop gs://fc-your-bucket/inputs/
```

#### Upload an entire folder of raw files

```bash
gcloud storage cp -r /path/to/your/data/folder/ gs://fc-your-bucket/raw-data/
```

The `-r` flag means "recursive" — it will copy the entire folder and all its contents.

#### Verify the upload

```bash
gcloud storage ls gs://fc-your-bucket/raw-data/
```

This lists all files in that folder so you can confirm everything uploaded correctly.

#### Uploading from an Instrument Computer (via TeamViewer)

If your raw files are on an instrument computer that you access remotely via TeamViewer:

1. Remote into the instrument computer using TeamViewer.
2. Make sure gcloud CLI is installed on the instrument computer (follow the Windows instructions above if not).
3. On the instrument computer, open **Windows Command Prompt** and run `gcloud auth login` to authenticate with your own Broad account.
4. Find the GCS destination path by browsing to your Terra bucket **on your own laptop**, then copy the destination folder path.
5. Back in the TeamViewer window, paste the GCS path and run the copy command:
   ```powershell
   gcloud storage cp -r "C:\path\to\instrument\data\" gs://fc-your-bucket/raw-data/
   ```
   > **TeamViewer clipboard tip:** If paste (`Ctrl+V`) does not work, go to *Actions → Clipboard → Paste as keystrokes* in the TeamViewer toolbar.

---

### Step 3: Add the Workflow to Your Terra Workspace

1. Log into [Terra](https://app.terra.bio) and navigate to your workspace.
2. Click the **"Workflows"** tab in the left navigation panel.
3. Click **"Find a Workflow"** (or the "+" button) to browse the workflow library.
4. Search for **`parallel_spectronaut`** (or the workflow name as published by the Proteogenomics team).
5. Click on the workflow, then click **"Export to Workspace"** and select your workspace.
6. The workflow will now appear in your workspace's Workflows tab.

---

### Step 4: Configure and Launch the Search

1. In your workspace, go to the **"Workflows"** tab.
2. Click on `parallel_spectronaut`.
3. You will see the workflow configuration page with three important options at the top:
   - **Snapshot version** — Always use the latest snapshot version for the most up-to-date features and bug fixes.
   - **Use call caching** — This is very useful. When checked, Terra remembers the results of each task. If your job fails halfway and you resubmit, it will skip tasks that already completed successfully and resume from the failure point. **Check this box** unless you specifically want to re-run everything from scratch (e.g., if you changed your search settings file but kept the exact same filename — Terra uses filenames to decide whether to reuse cached results).
   - **Inputs** — This is where you fill in all the input variables.

4. Fill in the required inputs at minimum:
   - `experiment_name` — a descriptive name for your run
   - `experiment_type` — `"proteome"` or `"ptm"`
   - `fasta_1` — path to your FASTA file (e.g., `gs://fc-your-bucket/inputs/human.fasta`)
   - `file_directory` — GCS path to the folder with your raw files (e.g., `gs://fc-your-bucket/raw-data/`)
   - `directDIA_settings` — path to your settings `.prop` file
   - `num_vms` — number of VMs (e.g., `8` for parallel, `1` for single-VM)

5. Click **"Run Analysis"** (or "Submit") to launch the workflow.

You can monitor progress in the **"Job History"** tab.

---

### Step 5: Collect Your Results

Once the workflow completes successfully:

- **Final results** (`spectronaut_output.zip`) are in the `call-combine_sne/` folder of your job output.
  This ZIP contains your merged SNE file and any reports you requested.

- **Merged search archive** (`merged_library.kit`) is in the `call-combine_final_archives/` folder.
  You can save this for future DIA analyses using the same library.

- **HTRMS converted files** (if conversion was enabled) are in the `call-htrms_conversion/` folder.
  To copy all converted files to your own GCS bucket:
  ```bash
  gcloud storage cp -r "gs://fc-your-bucket/submissions/YOUR-JOB-ID/call-htrms_conversion/**.htrms" gs://fc-your-bucket/htrms-output/
  ```

To download results to your local machine:
```bash
gcloud storage cp gs://fc-your-bucket/submissions/YOUR-JOB-ID/call-combine_sne/spectronaut_output.zip ./
```

You can find the exact output path by browsing your Terra workspace bucket in the Terra interface or in Google Cloud Storage.

---

## Troubleshooting

**The job failed partway through — how do I restart?**
Make sure **"Use call caching"** is checked in the workflow configuration. Then simply resubmit the job with the same inputs. Terra will skip all tasks that already completed and only re-run the failed task(s).

**I got an error about insufficient disk space.**
Increase the `disk_size_multiplier` input (e.g., from `3` to `4` or `5`). If the error was in the SNE combine step specifically, increase `sne_combine_disk_size_multiplier`.

**I got a permission error when uploading files — "You do not have access to this bucket".**
Re-authenticate your machine with your Broad Google account using `gcloud auth login`. This is the most common cause: another user previously signed in on the same computer, and their credentials are being used instead of yours. See the [authentication section](#step-1-set-up-gcloud-cli-and-authenticate) above for details.

**My job ran but I got no output or `.sne` files.**
Check the logs for the `dia_analysis_binned` task. Confirm that your `directDIA_settings` file is valid and compatible with your Spectronaut version (`v20.4`).

**I'm not sure how many VMs to use.**
A reasonable starting point is one VM per 20–30 files. For 200 files, `num_vms = 8` or `num_vms = 10` works well. The workflow will not create more VMs than you have files — if you request 20 VMs for 15 files, it will automatically scale down to 15 VMs.

**The workflow says it is using `"proteome"` settings even though I typed `"ptm"`.**
Double-check that you entered the value exactly as `"ptm"` (all lowercase, no spaces). Any unrecognized value for `experiment_type` will silently fall back to `"proteome"`.

---

## Contacts

- **C. Lian** — glian@broadinstitute.org
- **D. R. Mani** — proteogenomics@broadinstitute.org
- **Broad Institute Proteogenomics Team** — proteogenomics@broadinstitute.org

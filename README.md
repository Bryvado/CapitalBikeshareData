# Capital Bikeshare Data Pipeline

A modular, production-ready R pipeline that monitors S3 for new Capital Bikeshare trip-data ZIP files, downloads and cleans them, enriches missing coordinates via the GBFS station feed, and appends validated data to master parquet datasets.

The pipeline supports two storage backends:

| Mode | When active | Artefact location |
|---|---|---|
| **Local** (default) | `CBS_S3_BUCKET` not set | `data/`, `manifest.csv`, `logs/` on disk |
| **S3** | `CBS_S3_BUCKET` set | All artefacts in your S3 bucket |

---

## Folder structure

```
CapitalBikeshareData/
├── .github/
│   └── workflows/
│       └── monthly_pipeline.yml  # Scheduled GitHub Actions workflow
├── R/
│   ├── utils.R          # Logging, retry, path helpers
│   ├── storage.R        # S3 abstraction layer (use_s3(), key helpers, locking)
│   ├── downloader.R     # URL prediction, S3 polling, download, unzip
│   ├── schema.R         # Schema detection, standardisation, validation
│   ├── enrichment.R     # GBFS station reference, coordinate fill, ride_type
│   ├── parquet.R        # Manifest, per-month parquet, master append
│   ├── pipeline.R       # Top-level orchestrator (calls all modules)
│   ├── analysis.R       # Data availability summary + census-tract trip map
│   └── scheduler.R      # cronR / taskscheduleR scheduling helpers
├── data/
│   ├── raw/             # Downloaded ZIPs and extracted CSVs (per label sub-dir)
│   ├── processed/       # Per-month cleaned parquet files
│   ├── master/          # old_era.parquet / new_era.parquet
│   └── station_reference.csv   # GBFS station cache (auto-refreshed)
├── logs/
│   └── pipeline.log     # Unified log file (appended each run)
├── manifest.csv         # Processing manifest (idempotency record)
└── run_cbs_pipeline.R   # Auto-generated monthly driver script
```

In S3 mode the bucket mirrors the same structure:

```
s3://<CBS_S3_BUCKET>/<CBS_S3_PREFIX>/
├── data/
│   ├── processed/<label>/<label>_trips.parquet
│   ├── master/old_era.parquet
│   ├── master/new_era.parquet
│   └── station_reference.csv
├── manifest.csv
└── locks/pipeline.lock   # Ephemeral run lock (deleted after each run)
```

---

## Required R packages

Install once:

```r
install.packages(c(
  # Pipeline
  "httr2",        # HTTP requests (S3 polling + GBFS)
  "arrow",        # Parquet I/O
  "dplyr",        # Data manipulation
  "readr",        # CSV I/O
  "lubridate",    # Datetime helpers
  "stringr",      # String utilities
  "stringdist",   # Fuzzy station-name matching
  "jsonlite",     # GBFS JSON parsing
  "logger",       # Structured logging
  "fs",           # File-system utilities
  "tibble",       # Tibble construction
  # Analysis (R/analysis.R)
  "sf",           # Spatial data manipulation
  "tigris",       # US Census tract / boundary downloads
  "ggplot2",      # Plotting
  "scales",       # Axis / legend label formatting
  "tidyr",        # replace_na
  # S3 backend (only needed when CBS_S3_BUCKET is set):
  "paws.storage", # AWS SDK for R — S3 operations
  # Scheduling (choose one based on OS):
  "cronR",        # Linux / macOS cron scheduling
  "taskscheduleR" # Windows Task Scheduler
))
```

---

## Quick start

### Run the pipeline for the next expected month

```r
source("R/pipeline.R")
run_pipeline()
```

### Run the pipeline for a specific month

```r
source("R/pipeline.R")
run_pipeline(year = 2024, month = 5)
```

### Run for a historic annual file (2010–2017)

```r
source("R/downloader.R")
source("R/pipeline.R")
# Early historical files use year-only naming; pass month=1 as placeholder
run_pipeline(year = 2012, month = 1)
```

### Download and process all available data

```r
source("R/pipeline.R")
run_pipeline(all_available = TRUE)
```

---

## Analysis — data availability & census-tract map

After the pipeline has populated the master parquet files, the analysis module
(`R/analysis.R`) can:

1. Read the manifest and parquet files to produce a **year × month availability
   heatmap** showing how many trips are in each processed period.
2. Determine the **geographic extent** of all trip start/end coordinates.
3. Download the **US Census tracts** that overlap that extent via
   [`tigris`](https://github.com/walkerke/tigris).
4. Spatially join trip starts to tracts and produce a **choropleth map** of
   trip density.

Both plots are saved as PNGs in `data/plots/`.

### Run the full analysis

```r
source("R/analysis.R")
results <- run_analysis()
```

### Run for specific years only

```r
source("R/analysis.R")
results <- run_analysis(year_filter = 2022:2024)
```

### Access individual components

```r
source("R/analysis.R")

# 1. What year-months are available, and how many trips each?
avail <- summarize_data_availability()
print(avail)

# 2. Bounding box of all trip coordinates
ext <- trip_extent()

# 3. Download census tracts for the service area
tracts <- download_census_tracts(extent = ext, year = 2020)

# 4. Aggregate trip starts to tracts
tracts_with_counts <- aggregate_trips_to_tracts(tracts_sf = tracts)

# 5. Build plots manually
chart <- build_availability_chart(avail)
map   <- build_tract_map(tracts_with_counts)
```

### `run_analysis()` parameters

| Parameter | Default | Description |
|---|---|---|
| `root` | `"."` | Project root |
| `tract_year` | `2020` | Decennial census year for tract boundaries |
| `year_filter` | `NULL` | Integer vector of years to map; NULL = all years |
| `plots_dir` | `data/plots/` | Output directory for saved PNGs |
| `sample` | `1000000` | Max rows per era in `run_analysis()` (passed to aggregation & extent steps; `trip_extent()` uses `500000` by default when called directly) |

---

## Running without a local machine — GitHub Actions + S3

The pipeline can run fully unattended on GitHub Actions, writing every
artefact to an S3 bucket so nothing needs to be kept on your machine.

### 1 · Create an S3 bucket

Create a private S3 bucket in any AWS region.  The IAM user (or role) you
create for the pipeline needs the following permissions on that bucket:

```json
{
  "Effect": "Allow",
  "Action": [
    "s3:GetObject",
    "s3:PutObject",
    "s3:HeadObject",
    "s3:DeleteObject",
    "s3:ListBucket"
  ],
  "Resource": [
    "arn:aws:s3:::<your-bucket>",
    "arn:aws:s3:::<your-bucket>/*"
  ]
}
```

### 2 · Add GitHub secrets and variables

Go to **Settings → Secrets and variables → Actions** in your fork and add:

| Kind | Name | Value |
|---|---|---|
| Secret | `CBS_S3_BUCKET` | Your bucket name, e.g. `my-bikeshare-data` |
| Secret | `AWS_ACCESS_KEY_ID` | IAM access key |
| Secret | `AWS_SECRET_ACCESS_KEY` | IAM secret key |
| Variable | `AWS_REGION` | Bucket region, e.g. `us-east-1` |
| Variable | `CBS_S3_PREFIX` | *(optional)* key prefix, e.g. `cbs/prod` |

### 3 · Enable the workflow

The workflow file `.github/workflows/monthly_pipeline.yml` is already
committed.  It fires automatically at **00:01 UTC on the 1st of every
month**.

You can also trigger it manually from the **Actions** tab with optional
`year` and `month` inputs to backfill a specific period.

### 4 · S3 artefact layout

```
s3://<CBS_S3_BUCKET>/<CBS_S3_PREFIX>/
├── manifest.csv                              ← idempotency record
├── data/
│   ├── station_reference.csv                 ← GBFS station cache
│   ├── processed/<label>/<label>_trips.parquet
│   └── master/
│       ├── old_era.parquet
│       └── new_era.parquet
└── locks/pipeline.lock                       ← ephemeral run lock
```

### Using S3 mode locally

Set the same environment variables before calling `run_pipeline()`:

```r
Sys.setenv(
  CBS_S3_BUCKET         = "my-bikeshare-data",
  AWS_REGION            = "us-east-1",
  AWS_ACCESS_KEY_ID     = "AKIA...",
  AWS_SECRET_ACCESS_KEY = "..."
)
source("R/pipeline.R")
run_pipeline()
```

---

## Scheduling (Linux / macOS — cronR)

```r
source("R/scheduler.R")

# Install cron job (fires 00:01 on the 1st of every month)
schedule_with_cronr(project_root = fs::path_abs("."))

# Check installed jobs
show_schedule()

# Remove the job
unschedule_with_cronr()
```

## Scheduling (Windows — taskscheduleR)

```r
source("R/scheduler.R")
schedule_with_taskscheduler(project_root = fs::path_abs("."))
unschedule_with_taskscheduler()
```

---

## How it works

### 1 · URL prediction (`R/downloader.R`)

Capital Bikeshare uses two naming conventions:

| Period   | Pattern                                        |
|----------|------------------------------------------------|
| 2010–2017| `YYYY-capitalbikeshare-tripdata.zip`           |
| 2018+    | `YYYYMM-capitalbikeshare-tripdata.zip`         |

`next_expected_file()` advances the calendar by one month and builds the correct filename.  `poll_until_available()` then issues HTTP HEAD requests every 15 minutes until the server responds with 200.

### 2 · Schema standardisation (`R/schema.R`)

Two historical schemas are auto-detected and mapped to a **canonical** set of 15 columns:

| Column | Notes |
|---|---|
| `ride_id` | Bike# (old) or ride_id (new) |
| `rideable_type` | NA for old era |
| `started_at` / `ended_at` | POSIXct UTC, mixed-format parser |
| `duration_secs` | Derived from timestamps when raw field is missing |
| `start/end_station_name/id` | Character coerced |
| `start/end_lat/lng` | NA for old era — filled by enrichment |
| `user_type` | Normalised to "Member" / "Casual" |
| `source_file` | Originating CSV basename |
| `era` | "old" or "new" |

### 3 · Station enrichment (`R/enrichment.R`)

Missing coordinates are filled via a three-stage match against the live GBFS `station_information` feed (cached locally):

1. Exact `station_id` match
2. Exact `station_name` match (case-insensitive)
3. Fuzzy `station_name` match (Jaro-Winkler ≥ 0.92)

Four derived columns are then added:
- `station_based_start`, `station_based_end`
- `non_station_start`, `non_station_end`
- `ride_type`: `station-to-station` / `point-to-station` / `station-to-point` / `point-to-point`

### 4 · Parquet output (`R/parquet.R`)

Each month is written to `data/processed/<label>/<label>_trips.parquet`.

Two master datasets are maintained in `data/master/`:

| File | Contents |
|---|---|
| `old_era.parquet` | Pre-coordinate era; `start/end_lat/lng` filled by GBFS where possible |
| `new_era.parquet` | Modern era with native trip-level coordinates |

Both share the same column schema for interchangeable downstream analysis.

### 5 · Idempotency

The `manifest.csv` file records every attempted run with its status (`ok` or `error:<msg>`).  Before any work starts, `already_processed()` checks the manifest — if the file was previously processed successfully the pipeline exits immediately.

Within the master parquet append, rows whose `source_file` already exists are removed before re-inserting, so re-running a month never creates duplicates.

---

## Configuration

All tunable parameters are arguments to `run_pipeline()`:

| Parameter | Default | Description |
|---|---|---|
| `year` / `month` | `NULL` (auto) | Target period |
| `all_available` | `FALSE` | Process every currently available file (cannot be combined with `year` / `month`) |
| `root` | `"."` | Project root |
| `poll_interval` | `900` | Seconds between S3 polls |
| `poll_timeout` | 30 days | Give up after this many seconds |
| `fuzzy_threshold` | `0.92` | Jaro-Winkler cutoff for fuzzy station matching |

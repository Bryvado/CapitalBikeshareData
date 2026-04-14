# Capital Bikeshare Data Pipeline

A modular, production-ready R pipeline that monitors S3 for new Capital Bikeshare trip-data ZIP files, downloads and cleans them, enriches missing coordinates via the GBFS station feed, and appends validated data to master parquet datasets.

---

## Folder structure

```
CapitalBikeshareData/
├── R/
│   ├── utils.R          # Logging, retry, path helpers
│   ├── downloader.R     # URL prediction, S3 polling, download, unzip
│   ├── schema.R         # Schema detection, standardisation, validation
│   ├── enrichment.R     # GBFS station reference, coordinate fill, ride_type
│   ├── parquet.R        # Manifest, per-month parquet, master append
│   ├── pipeline.R       # Top-level orchestrator (calls all modules)
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

---

## Required R packages

Install once:

```r
install.packages(c(
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

### Run for the historic 2014 annual file

```r
source("R/downloader.R")
source("R/pipeline.R")
# The 2014 file uses year-only naming; pass year=2014, month=1
run_pipeline(year = 2014, month = 1)
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

Capital Bikeshare uses three naming conventions:

| Period   | Pattern                                        |
|----------|------------------------------------------------|
| 2014     | `2014-capitalbikeshare-tripdata.zip`           |
| 2015–2017| `YYYYQN-capitalbikeshare-tripdata.zip`         |
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
| `root` | `"."` | Project root |
| `poll_interval` | `900` | Seconds between S3 polls |
| `poll_timeout` | 30 days | Give up after this many seconds |
| `fuzzy_threshold` | `0.92` | Jaro-Winkler cutoff for fuzzy station matching |

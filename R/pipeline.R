# R/pipeline.R
# Top-level pipeline orchestrator: ties all modules together.
# ---------------------------------------------------------------------------
# Usage:
#   source("R/pipeline.R")
#   run_pipeline()                           # process next expected month
#   run_pipeline(year = 2024, month = 3)     # process a specific month
# ---------------------------------------------------------------------------

suppressPackageStartupMessages({
  library(logger)
  library(fs)
})

source("R/storage.R")
source("R/utils.R")
source("R/downloader.R")
source("R/schema.R")
source("R/enrichment.R")
source("R/parquet.R")

# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

#' Run the full Capital Bikeshare data pipeline for one month.
#'
#' Steps:
#'   1. Determine which month to process (auto or supplied).
#'   2. Check the manifest — skip if already done.
#'   3. Poll S3 until the ZIP is available.
#'   4. Download and unzip.
#'   5. Read and standardise every CSV in the ZIP.
#'   6. Validate the combined data.
#'   7. Fetch station reference and enrich (fill coords, add ride_type).
#'   8. Write a per-month parquet.
#'   9. Append to the master dataset.
#'  10. Update the manifest.
#'
#' @param year             Integer year to process, or NULL to auto-detect next.
#' @param month            Integer month (1-12), or NULL to auto-detect.
#' @param all_available    Logical; when TRUE, process every available file.
#' @param root             Project root directory.
#' @param poll_interval    Seconds between S3 existence polls (default 900).
#' @param poll_timeout     Total seconds to wait for the file (default 30 days).
#' @param fuzzy_threshold  Jaro-Winkler threshold for station fuzzy-matching.
#' @return Invisibly, the path to the per-month parquet that was written.
#'         Returns NULL if the month was already processed (skipped).
#'         When `all_available = TRUE`, returns a character vector of outputs.
#' @export
run_pipeline <- function(year           = NULL,
                         month          = NULL,
                         all_available  = FALSE,
                         root           = ".",
                         poll_interval  = 900L,
                         poll_timeout   = 30L * 24L * 3600L,
                         fuzzy_threshold = 0.92) {

  init_logger(log_dir = file.path(root, "logs"))
  logger::log_info("=== Capital Bikeshare Pipeline START ===")

  if (isTRUE(all_available)) {
    if (!is.null(year) || !is.null(month)) {
      stop("`all_available = TRUE` cannot be combined with `year` or `month`.")
    }

    targets <- available_cbs_files()
    if (length(targets) == 0L) {
      logger::log_warn("No available Capital Bikeshare files were found.")
      return(invisible(character()))
    }

    logger::log_info("Processing all available files: {length(targets)} target(s)")
    outputs <- unlist(lapply(targets, function(target) {
      run_pipeline(
        year            = target$year,
        month           = target$month,
        all_available   = FALSE,
        root            = root,
        poll_interval   = poll_interval,
        poll_timeout    = poll_timeout,
        fuzzy_threshold = fuzzy_threshold
      )
    }), use.names = FALSE)

    return(invisible(outputs))
  }

  # ------------------------------------------------------------------
  # Run-level locking (S3 mode only) — prevents concurrent executions.
  # The lock is released automatically via on.exit even on error.
  # ------------------------------------------------------------------
  if (use_s3()) {
    if (!acquire_run_lock()) {
      stop("Another pipeline run is currently in progress — exiting.")
    }
    on.exit(release_run_lock(), add = TRUE)
  }

  # ------------------------------------------------------------------
  # 1. Determine target file
  # ------------------------------------------------------------------
  if (!is.null(year) && !is.null(month)) {
    year  <- as.integer(year)
    month <- as.integer(month)
    fname <- cbs_filename(year, month)
    url   <- cbs_url(fname)
  } else {
    target <- next_expected_file(year, month)
    year   <- target$year
    month  <- target$month
    fname  <- target$filename
    url    <- target$url
  }
  label <- sprintf("%04d%02d", year, month)
  logger::log_info("Target: {fname} ({url})")

  # ------------------------------------------------------------------
  # 2. Idempotency check
  # ------------------------------------------------------------------
  mf_path <- manifest_path(root)
  if (already_processed(fname, mf_path)) {
    logger::log_info("Already processed: {fname} — skipping")
    return(invisible(NULL))
  }
  downloaded_at <- Sys.time()

  dest_raw <- raw_dir(label, root)
  zip_file <- file.path(dest_raw, fname)

  # ------------------------------------------------------------------
  # 3. Poll until available (unless already downloaded locally)
  # ------------------------------------------------------------------
  already_downloaded <- fs::file_exists(zip_file)

  if (already_downloaded) {
    logger::log_info("ZIP already downloaded locally: {zip_file} — skipping availability poll")
  } else {
    poll_until_available(url,
                         interval_secs = poll_interval,
                         timeout_secs  = poll_timeout)
  }

  # ------------------------------------------------------------------
  # 4. Download and unzip
  # ------------------------------------------------------------------
  extracted_files <- if (already_downloaded) {
    unzip_existing_zip(zip_file, dest_raw)
  } else {
    download_and_unzip(url, dest_raw)
  }
  csv_files <- extracted_files[grepl("\\.csv$", extracted_files,
                                     ignore.case = TRUE)]
  is_macos_metadata <- startsWith(basename(csv_files), "._")
  skipped_metadata <- sum(is_macos_metadata)
  csv_files <- csv_files[!is_macos_metadata]
  if (skipped_metadata > 0L) {
    logger::log_info("Ignoring {skipped_metadata} macOS metadata CSV file(s) (._*)")
  }
  if (length(csv_files) == 0L)
    stop(sprintf("No valid CSV files found in ZIP for %s", fname))

  logger::log_info("Found {length(csv_files)} valid CSV file(s)")

  # ------------------------------------------------------------------
  # 5. Read and standardise all CSVs
  # ------------------------------------------------------------------
  trips_list <- lapply(csv_files, function(f) {
    tryCatch(
      read_trip_csv(f),
      error = function(e) {
        logger::log_error("Failed to read {basename(f)}: {conditionMessage(e)}")
        NULL
      }
    )
  })
  trips_list <- Filter(Negate(is.null), trips_list)
  if (length(trips_list) == 0L)
    stop("All CSVs failed to parse")

  trips <- dplyr::bind_rows(trips_list)
  era   <- unique(trips$era)
  if (length(era) > 1L) {
    # Keep the era from the first file (edge-case: annual zip spanning eras)
    dominant_era <- names(sort(table(trips$era), decreasing = TRUE))[1L]
    logger::log_warn("Multiple eras detected; treating as [{dominant_era}]")
    trips$era <- dominant_era
    era <- dominant_era
  }
  logger::log_info("Parsed {nrow(trips)} rows, era={era}")

  # ------------------------------------------------------------------
  # 6. Validate
  # ------------------------------------------------------------------
  validation <- validate_trips(trips)
  if (!validation$pass) {
    status_msg <- paste("error:", paste(validation$messages, collapse = "; "))
    logger::log_error("Validation failed: {status_msg}")
    append_manifest(mf_path, fname, url, era,
                    downloaded_at, Sys.time(), nrow(trips), status_msg)
    stop(status_msg)
  }
  logger::log_info("Validation passed")

  # ------------------------------------------------------------------
  # 7. Enrich: station reference + coordinate fill + ride_type flags
  # ------------------------------------------------------------------
  station_ref <- tryCatch(
    fetch_station_reference(
      cache_path = file.path(root, "data", "station_reference.csv")
    ),
    error = function(e) {
      logger::log_warn("Station reference unavailable — enrichment skipped: ",
                       conditionMessage(e))
      NULL
    }
  )

  if (!is.null(station_ref)) {
    trips <- enrich_trips(trips, station_ref,
                          fuzzy_threshold = fuzzy_threshold)
  } else {
    trips <- add_ride_type_flags(trips)
  }

  # ------------------------------------------------------------------
  # 8. Write per-month parquet
  # ------------------------------------------------------------------
  dest_proc   <- processed_dir(label, root)
  parquet_out <- write_processed_parquet(trips, dest_proc, label)

  # ------------------------------------------------------------------
  # 9. Append to master
  # ------------------------------------------------------------------
  append_to_master(trips, root)

  # ------------------------------------------------------------------
  # 10. Update manifest
  # ------------------------------------------------------------------
  append_manifest(mf_path, fname, url, era,
                  downloaded_at, Sys.time(), nrow(trips), "ok")

  logger::log_info("=== Pipeline COMPLETE for {label}: {nrow(trips)} rows ===")
  invisible(parquet_out)
}

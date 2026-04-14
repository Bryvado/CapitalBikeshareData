# R/parquet.R
# Manifest tracking, parquet I/O, and master dataset append/validation
# ---------------------------------------------------------------------------
# Manifest CSV columns:
#   filename, url, era, downloaded_at, processed_at, rows, status
# ---------------------------------------------------------------------------

suppressPackageStartupMessages({
  library(arrow)
  library(dplyr)
  library(readr)
  library(fs)
  library(logger)
})

source("R/utils.R")
source("R/storage.R")

MANIFEST_COLS <- c(
  "filename", "url", "era", "downloaded_at", "processed_at",
  "rows", "status"                     # status: "ok" | "error:<msg>"
)

# ---------------------------------------------------------------------------
# Manifest helpers
# ---------------------------------------------------------------------------

#' Read the processing manifest, creating it if absent.
#'
#' In S3 mode (`CBS_S3_BUCKET` set) the manifest is read from S3.
#' In local mode the manifest CSV is read from `path` (created if absent).
#'
#' @param path  Path to the local manifest CSV (from `manifest_path()`).
#' @return A tibble with MANIFEST_COLS.
#' @export
read_manifest <- function(path = manifest_path()) {
  empty_mf <- tibble::tibble(
    filename      = character(),
    url           = character(),
    era           = character(),
    downloaded_at = as.POSIXct(character()),
    processed_at  = as.POSIXct(character()),
    rows          = integer(),
    status        = character()
  )

  if (use_s3()) {
    key <- s3_key_manifest()
    if (!s3_object_exists(key)) return(empty_mf)
    return(s3_read_csv(key))
  }

  # --- local mode ---
  if (!fs::file_exists(path)) {
    readr::write_csv(empty_mf, path)
    return(empty_mf)
  }
  readr::read_csv(path, show_col_types = FALSE)
}

#' Append one row to the manifest.
#'
#' In S3 mode the manifest is read from S3, the new entry is bound on, and
#' the full CSV is written back.  In local mode `readr::write_csv` appends
#' directly to the file.
#'
#' @param path          Path to the local manifest CSV.
#' @param filename      ZIP file name (key).
#' @param url           Source URL.
#' @param era           "old" or "new".
#' @param downloaded_at POSIXct timestamp of download.
#' @param processed_at  POSIXct timestamp of processing.
#' @param rows          Number of rows written.
#' @param status        "ok" or "error:<message>".
#' @export
append_manifest <- function(path,
                            filename, url, era,
                            downloaded_at, processed_at,
                            rows, status) {
  entry <- tibble::tibble(
    filename      = filename,
    url           = url,
    era           = era,
    downloaded_at = downloaded_at,
    processed_at  = processed_at,
    rows          = as.integer(rows),
    status        = status
  )

  if (use_s3()) {
    existing <- read_manifest()
    updated  <- dplyr::bind_rows(existing, entry)
    s3_write_csv(updated, s3_key_manifest())
    return(invisible(entry))
  }

  # --- local mode ---
  readr::write_csv(entry, path, append = fs::file_exists(path))
  invisible(entry)
}

#' Check if a ZIP file has already been successfully processed.
#'
#' @param filename  ZIP file name to look up.
#' @param path      Path to manifest CSV.
#' @return TRUE if found with status "ok".
#' @export
already_processed <- function(filename, path = manifest_path()) {
  mf <- read_manifest(path)
  any(mf$filename == filename & mf$status == "ok")
}

# ---------------------------------------------------------------------------
# Parquet helpers
# ---------------------------------------------------------------------------

#' Write a trip tibble to a dated parquet file in `dest_dir`.
#'
#' The parquet is always written locally.  When S3 mode is active it is also
#' uploaded to the bucket under the canonical key for this period.
#'
#' @param df       Trip tibble (canonical + enrichment columns).
#' @param dest_dir Directory for local parquet outputs.
#' @param label    Period label (e.g. "202405") used in the file name.
#' @return Path to the locally written parquet file.
#' @export
write_processed_parquet <- function(df, dest_dir, label) {
  fs::dir_create(dest_dir)
  out_path <- file.path(dest_dir, paste0(label, "_trips.parquet"))
  arrow::write_parquet(df, out_path)
  logger::log_info("Wrote {nrow(df)} rows to {out_path}")

  if (use_s3()) {
    key <- s3_key_processed(label)
    s3_upload_file(out_path, key)
    logger::log_info("Uploaded processed parquet to s3://{s3_bucket()}/{key}")
  }

  invisible(out_path)
}

# ---------------------------------------------------------------------------
# Master dataset append
# ---------------------------------------------------------------------------

#' Append a processed trip tibble to the correct master parquet file.
#'
#' In S3 mode the master is read from / written to the S3 bucket.
#' In local mode the original atomic temp-file pattern is used.
#'
#' The era ("old" or "new") is taken from the `era` column of `df`.
#'
#' Idempotency: rows whose `source_file` already appears in the master are
#' removed before appending (so re-running the same month is safe).
#'
#' @param df   Enriched trip tibble containing an `era` column.
#' @param root Project root directory (used in local mode only).
#' @export
append_to_master <- function(df, root = ".") {
  era <- unique(df$era)
  if (length(era) > 1L)
    stop("df contains rows from multiple eras; split before calling append_to_master()")
  era <- era[1L]

  new_source_files <- unique(df$source_file)

  if (use_s3()) {
    key <- s3_key_master(era)
    if (s3_object_exists(key)) {
      master <- s3_read_parquet(key)
      master <- dplyr::filter(master, !source_file %in% new_source_files)
    } else {
      master <- tibble::tibble()
    }
    combined <- dplyr::bind_rows(master, df)
    s3_write_parquet(combined, key)
    logger::log_info("Master [{era}] updated in S3: {nrow(combined)} total rows ",
                     "(+{nrow(df)} new)")
    return(invisible(key))
  }

  # --- local mode ---
  mpath <- master_path(era, root)
  fs::dir_create(dirname(mpath))

  if (fs::file_exists(mpath)) {
    master <- arrow::read_parquet(mpath)
    # Remove any previously written rows for these source files (idempotency)
    master <- dplyr::filter(master, !source_file %in% new_source_files)
  } else {
    master <- tibble::tibble()
  }

  combined <- dplyr::bind_rows(master, df)

  # Atomic write via temp file
  tmp <- paste0(mpath, ".tmp")
  arrow::write_parquet(combined, tmp)
  fs::file_move(tmp, mpath)

  logger::log_info("Master [{era}] updated: {nrow(combined)} total rows ",
                   "(+{nrow(df)} new)")
  invisible(mpath)
}

# ---------------------------------------------------------------------------
# Read master datasets
# ---------------------------------------------------------------------------

#' Read a master parquet dataset.
#'
#' In S3 mode the master is read from the S3 bucket.
#' In local mode it is read from the local `data/master/` directory.
#'
#' @param era  "old" or "new".
#' @param root Project root (local mode only).
#' @return Arrow Table or tibble, or NULL if the master does not exist yet.
#' @export
read_master <- function(era = c("new", "old"), root = ".") {
  era <- match.arg(era)

  if (use_s3()) {
    key <- s3_key_master(era)
    if (!s3_object_exists(key)) {
      logger::log_warn("Master [{era}] does not exist yet in S3")
      return(NULL)
    }
    return(s3_read_parquet(key))
  }

  # --- local mode ---
  mpath <- master_path(era, root)
  if (!fs::file_exists(mpath)) {
    logger::log_warn("Master [{era}] does not exist yet at {mpath}")
    return(NULL)
  }
  arrow::read_parquet(mpath)
}

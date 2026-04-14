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

MANIFEST_COLS <- c(
  "filename", "url", "era", "downloaded_at", "processed_at",
  "rows", "status"                     # status: "ok" | "error:<msg>"
)

# ---------------------------------------------------------------------------
# Manifest helpers
# ---------------------------------------------------------------------------

#' Read the processing manifest, creating it if absent.
#'
#' @param path  Path to the manifest CSV (from `manifest_path()`).
#' @return A tibble with MANIFEST_COLS.
#' @export
read_manifest <- function(path = manifest_path()) {
  if (!fs::file_exists(path)) {
    df <- tibble::tibble(
      filename      = character(),
      url           = character(),
      era           = character(),
      downloaded_at = as.POSIXct(character()),
      processed_at  = as.POSIXct(character()),
      rows          = integer(),
      status        = character()
    )
    readr::write_csv(df, path)
    return(df)
  }
  readr::read_csv(path, show_col_types = FALSE)
}

#' Append one row to the manifest CSV.
#'
#' @param path          Path to the manifest CSV.
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
  # write_csv in append mode
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
#' @param df       Trip tibble (canonical + enrichment columns).
#' @param dest_dir Directory for parquet outputs.
#' @param label    Period label (e.g. "202405") used in the file name.
#' @return Path to the written parquet file.
#' @export
write_processed_parquet <- function(df, dest_dir, label) {
  fs::dir_create(dest_dir)
  out_path <- file.path(dest_dir, paste0(label, "_trips.parquet"))
  arrow::write_parquet(df, out_path)
  logger::log_info("Wrote {nrow(df)} rows to {out_path}")
  invisible(out_path)
}

# ---------------------------------------------------------------------------
# Master dataset append
# ---------------------------------------------------------------------------

#' Append a processed trip tibble to the correct master parquet file.
#'
#' The era ("old" or "new") is taken from the `era` column of `df`.
#' The master parquet is read (if it exists), the new rows are bound on,
#' and the whole file is re-written atomically via a temp file.
#'
#' Idempotency: rows whose `source_file` already appears in the master are
#' removed before appending (so re-running the same month is safe).
#'
#' @param df   Enriched trip tibble containing an `era` column.
#' @param root Project root directory.
#' @export
append_to_master <- function(df, root = ".") {
  era   <- unique(df$era)
  if (length(era) > 1L)
    stop("df contains rows from multiple eras; split before calling append_to_master()")
  era <- era[1L]

  mpath <- master_path(era, root)
  fs::dir_create(dirname(mpath))

  # Columns present in df (master uses a superset)
  new_source_files <- unique(df$source_file)

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
#' @param era  "old" or "new".
#' @param root Project root.
#' @return Arrow Table or tibble.
#' @export
read_master <- function(era = c("new", "old"), root = ".") {
  era   <- match.arg(era)
  mpath <- master_path(era, root)
  if (!fs::file_exists(mpath)) {
    logger::log_warn("Master [{era}] does not exist yet at {mpath}")
    return(NULL)
  }
  arrow::read_parquet(mpath)
}

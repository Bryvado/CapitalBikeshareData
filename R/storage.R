# R/storage.R
# S3 storage backend for the Capital Bikeshare pipeline.
# ---------------------------------------------------------------------------
# When the environment variable CBS_S3_BUCKET is set the pipeline stores all
# artefacts (manifest, processed parquet, master parquet, station-reference
# cache) in that S3 bucket instead of on the local filesystem.
#
# Required environment variables (S3 mode):
#   CBS_S3_BUCKET            – S3 bucket name
#   CBS_S3_PREFIX            – (optional) key prefix within the bucket
#   AWS_REGION               – AWS region, e.g. "us-east-1"
#   AWS_ACCESS_KEY_ID        – AWS access key  (or use an IAM role / OIDC)
#   AWS_SECRET_ACCESS_KEY    – AWS secret key
#
# Local mode (default when CBS_S3_BUCKET is unset) is fully backward
# compatible — no behaviour changes.
# ---------------------------------------------------------------------------

suppressPackageStartupMessages({
  library(fs)
  library(logger)
})

# ---------------------------------------------------------------------------
# Backend detection
# ---------------------------------------------------------------------------

#' Return TRUE when S3 backend is configured (CBS_S3_BUCKET env var is set).
#' @export
use_s3 <- function() nzchar(Sys.getenv("CBS_S3_BUCKET"))

#' Return the configured S3 bucket name.  Stops if not set.
#' @export
s3_bucket <- function() {
  b <- Sys.getenv("CBS_S3_BUCKET")
  if (!nzchar(b)) stop("CBS_S3_BUCKET environment variable is not set")
  b
}

#' Return the configured S3 key prefix (empty string by default).
#' @export
s3_prefix <- function() Sys.getenv("CBS_S3_PREFIX", unset = "")

#' Build a full S3 key from one or more path segments.
#'
#' Prepends CBS_S3_PREFIX if set and joins all non-empty segments with "/".
#'
#' @param ...  Path segments (character scalars).
#' @return     A single S3 key string.
#' @export
s3_key <- function(...) {
  parts <- c(s3_prefix(), ...)
  parts <- parts[nzchar(parts)]
  paste(parts, collapse = "/")
}

# ---------------------------------------------------------------------------
# S3 client (lazily created, cached for the R session)
# ---------------------------------------------------------------------------

.cbs_storage_env <- new.env(parent = emptyenv())
.cbs_storage_env$client <- NULL

#' Return a paws.storage S3 client, creating it on first call.
#' @export
s3_client <- function() {
  if (is.null(.cbs_storage_env$client)) {
    if (!requireNamespace("paws.storage", quietly = TRUE))
      stop(paste(
        "Package 'paws.storage' is required for S3 mode.",
        "Install with: install.packages('paws.storage')"
      ))
    .cbs_storage_env$client <- paws.storage::s3()
  }
  .cbs_storage_env$client
}

# ---------------------------------------------------------------------------
# Low-level S3 operations
# ---------------------------------------------------------------------------

#' Check whether a key exists in the S3 bucket.
#'
#' @param key  S3 object key.
#' @return     TRUE / FALSE.
#' @export
s3_object_exists <- function(key) {
  tryCatch({
    s3_client()$head_object(Bucket = s3_bucket(), Key = key)
    TRUE
  }, error = function(e) FALSE)
}

#' Upload a local file to S3.
#'
#' @param local_path  Path to the local file to upload.
#' @param key         Destination S3 object key.
#' @export
s3_upload_file <- function(local_path, key) {
  con <- file(local_path, open = "rb")
  on.exit(close(con), add = TRUE)
  s3_client()$put_object(
    Bucket = s3_bucket(),
    Key    = key,
    Body   = con
  )
  logger::log_debug("S3 upload: {local_path} → s3://{s3_bucket()}/{key}")
  invisible(key)
}

#' Download an S3 object to a local file.
#'
#' @param key         S3 object key to download.
#' @param local_path  Destination local path (parent directories are created).
#' @export
s3_download_file <- function(key, local_path) {
  fs::dir_create(dirname(local_path))
  resp <- s3_client()$get_object(Bucket = s3_bucket(), Key = key)
  writeBin(resp$Body, local_path)
  logger::log_debug("S3 download: s3://{s3_bucket()}/{key} → {local_path}")
  invisible(local_path)
}

#' Read an S3 object and return its content as a character string.
#'
#' @param key  S3 object key.
#' @return     Character scalar.
#' @export
s3_read_text <- function(key) {
  resp <- s3_client()$get_object(Bucket = s3_bucket(), Key = key)
  rawToChar(resp$Body)
}

#' Write a character string as an S3 object.
#'
#' @param text Character string.
#' @param key  Destination S3 object key.
#' @export
s3_write_text <- function(text, key) {
  s3_client()$put_object(
    Bucket = s3_bucket(),
    Key    = key,
    Body   = charToRaw(text)
  )
  invisible(key)
}

# ---------------------------------------------------------------------------
# CSV helpers (in-memory, no temp files needed)
# ---------------------------------------------------------------------------

#' Read a CSV stored in S3 and return it as a tibble.
#'
#' @param key  S3 object key.
#' @param ...  Additional arguments forwarded to `readr::read_csv`.
#' @return     A tibble.
#' @export
s3_read_csv <- function(key, ...) {
  text <- s3_read_text(key)
  readr::read_csv(I(text), show_col_types = FALSE, ...)
}

#' Write a data frame to S3 as CSV.
#'
#' @param df   Data frame / tibble.
#' @param key  Destination S3 object key.
#' @export
s3_write_csv <- function(df, key) {
  tmp <- tempfile(fileext = ".csv")
  on.exit(unlink(tmp), add = TRUE)
  readr::write_csv(df, tmp)
  s3_upload_file(tmp, key)
  invisible(key)
}

# ---------------------------------------------------------------------------
# Parquet helpers
# ---------------------------------------------------------------------------

#' Read a parquet file from S3 and return it as a tibble.
#'
#' @param key  S3 object key.
#' @return     A tibble.
#' @export
s3_read_parquet <- function(key) {
  tmp <- tempfile(fileext = ".parquet")
  on.exit(unlink(tmp), add = TRUE)
  s3_download_file(key, tmp)
  arrow::read_parquet(tmp)
}

#' Write a data frame to S3 as parquet.
#'
#' @param df   Data frame / tibble.
#' @param key  Destination S3 object key.
#' @export
s3_write_parquet <- function(df, key) {
  tmp <- tempfile(fileext = ".parquet")
  on.exit(unlink(tmp), add = TRUE)
  arrow::write_parquet(df, tmp)
  s3_upload_file(tmp, key)
  invisible(key)
}

# ---------------------------------------------------------------------------
# Canonical S3 key helpers  (mirror the local path helpers in utils.R)
# ---------------------------------------------------------------------------

#' S3 key for the processing manifest CSV.
#' @export
s3_key_manifest <- function() s3_key("manifest.csv")

#' S3 key for a master parquet dataset.
#'
#' @param era "old" or "new".
#' @export
s3_key_master <- function(era = c("new", "old")) {
  era <- match.arg(era)
  s3_key("data", "master", paste0(era, "_era.parquet"))
}

#' S3 key for a per-month processed parquet file.
#'
#' @param label Period label, e.g. "202405".
#' @export
s3_key_processed <- function(label) {
  s3_key("data", "processed", label, paste0(label, "_trips.parquet"))
}

#' S3 key for the station-reference CSV cache.
#' @export
s3_key_station_reference <- function() s3_key("data", "station_reference.csv")

#' S3 key for the run-lock sentinel object.
#' @export
s3_key_lock <- function() s3_key("locks", "pipeline.lock")

# ---------------------------------------------------------------------------
# Run-level locking (prevents concurrent pipeline runs)
# ---------------------------------------------------------------------------

#' Attempt to acquire a run lock via an S3 sentinel object.
#'
#' The lock is a small JSON object that records the start time and the GitHub
#' Actions run ID (if available).  Returns FALSE without error when the lock
#' is already held so the caller can decide what to do.
#'
#' @return TRUE on success; FALSE when the lock is already held.
#' @export
acquire_run_lock <- function() {
  key <- s3_key_lock()
  if (s3_object_exists(key)) {
    logger::log_warn("Run lock already held at s3://{s3_bucket()}/{key}")
    return(FALSE)
  }
  info <- jsonlite::toJSON(list(
    locked_at = format(Sys.time(), "%Y-%m-%dT%H:%M:%SZ"),
    runner    = Sys.getenv("GITHUB_RUN_ID", unset = "local")
  ), auto_unbox = TRUE)
  s3_write_text(info, key)
  logger::log_info("Run lock acquired: s3://{s3_bucket()}/{key}")
  TRUE
}

#' Release the run lock from S3.
#'
#' Silently succeeds even if the lock does not exist.
#'
#' @export
release_run_lock <- function() {
  key <- s3_key_lock()
  tryCatch(
    s3_client()$delete_object(Bucket = s3_bucket(), Key = key),
    error = function(e)
      logger::log_warn("Failed to release run lock: {conditionMessage(e)}")
  )
  logger::log_info("Run lock released")
  invisible(TRUE)
}

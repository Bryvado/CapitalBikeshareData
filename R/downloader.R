# R/downloader.R
# URL prediction, polling, download, and unzip for Capital Bikeshare data
# ---------------------------------------------------------------------------
# Known S3 base URL
#   https://s3.amazonaws.com/capitalbikeshare-data/
#
# File-name patterns (by year):
#   2014       → 2014-capitalbikeshare-tripdata.zip   (annual)
#   2015–2017  → YYYYQQ-capitalbikeshare-tripdata.zip (quarterly: Q1–Q4)
#   2018+      → YYYYMM-capitalbikeshare-tripdata.zip (monthly)
# ---------------------------------------------------------------------------

suppressPackageStartupMessages({
  library(httr2)
  library(fs)
  library(logger)
})

source("R/utils.R")

CBS_BASE_URL <- "https://s3.amazonaws.com/capitalbikeshare-data/"

# ---------------------------------------------------------------------------
# URL / label prediction
# ---------------------------------------------------------------------------

#' Return the S3 file-name for a given year and (optionally) month.
#'
#' For 2014 the annual ZIP is used; for 2015-2017 quarterly ZIPs are used;
#' for 2018+ monthly ZIPs are used.
#'
#' @param year  Integer year.
#' @param month Integer month (1-12). Ignored for 2014.
#' @return A character string with the ZIP file name (no base URL).
#' @export
cbs_filename <- function(year, month = NULL) {
  year <- as.integer(year)
  if (year == 2014L) {
    return("2014-capitalbikeshare-tripdata.zip")
  }
  if (is.null(month)) stop("month required for year != 2014")
  month <- as.integer(month)
  if (year >= 2018L) {
    label <- sprintf("%04d%02d", year, month)
  } else {
    # 2015-2017: quarterly files — map month → quarter label
    q <- ceiling(month / 3L)
    label <- sprintf("%04dQ%d", year, q)
  }
  paste0(label, "-capitalbikeshare-tripdata.zip")
}

#' Return the full S3 URL for a ZIP file.
#'
#' @param filename  Value returned by `cbs_filename()`.
#' @export
cbs_url <- function(filename) paste0(CBS_BASE_URL, filename)

#' Predict the next expected ZIP label given the latest known period.
#'
#' For 2018+ monthly files the next label is the calendar month after
#' `current_year / current_month`.  For quarterly 2015-2017 files the
#' next quarter is returned.  If `current_year` is 2014 the first
#' quarterly file of 2015 (Q1) is returned.
#'
#' @param current_year  Most-recently processed year (integer or NULL to use today).
#' @param current_month Most-recently processed month (integer or NULL).
#' @return A named list: list(year=, month=, filename=, url=).
#' @export
next_expected_file <- function(current_year = NULL, current_month = NULL) {
  if (is.null(current_year)) {
    today        <- Sys.Date()
    current_year  <- as.integer(format(today, "%Y"))
    current_month <- as.integer(format(today, "%m"))
  }
  current_year  <- as.integer(current_year)
  current_month <- as.integer(current_month %||% 1L)

  # Advance by one month
  next_month <- current_month + 1L
  next_year  <- current_year
  if (next_month > 12L) {
    next_month <- 1L
    next_year  <- next_year + 1L
  }

  fname <- cbs_filename(next_year, next_month)
  list(year     = next_year,
       month    = next_month,
       filename = fname,
       url      = cbs_url(fname))
}

# Null-coalescing operator (available in R 4.4+ as ??, replicated here)
`%||%` <- function(a, b) if (!is.null(a)) a else b

# ---------------------------------------------------------------------------
# Existence check
# ---------------------------------------------------------------------------

#' Check whether a URL exists on S3 using an HTTP HEAD request.
#'
#' @param url Character URL to probe.
#' @return TRUE if the server returns 200, FALSE otherwise.
#' @export
url_exists <- function(url) {
  resp <- tryCatch(
    httr2::request(url) |>
      httr2::req_method("HEAD") |>
      httr2::req_timeout(15) |>
      httr2::req_perform(),
    error = function(e) NULL
  )
  if (is.null(resp)) return(FALSE)
  httr2::resp_status(resp) == 200L
}

# ---------------------------------------------------------------------------
# Polling loop
# ---------------------------------------------------------------------------

#' Poll S3 every `interval_secs` seconds until `url` exists or `timeout_secs`
#' elapses.
#'
#' @param url           Full S3 URL to poll.
#' @param interval_secs Seconds between polls (default 900 = 15 min).
#' @param timeout_secs  Give up after this many seconds (default 30 days).
#' @return TRUE when the file becomes available; stops with an error on timeout.
#' @export
poll_until_available <- function(url,
                                 interval_secs = 900L,
                                 timeout_secs  = 30L * 24L * 3600L) {
  deadline <- Sys.time() + timeout_secs
  logger::log_info("Polling {url} every {interval_secs}s (timeout {timeout_secs}s)")

  repeat {
    if (url_exists(url)) {
      logger::log_info("File available: {url}")
      return(TRUE)
    }
    if (Sys.time() >= deadline) {
      stop(sprintf("Timed out waiting for %s after %d seconds", url, timeout_secs))
    }
    logger::log_debug("Not yet available — sleeping {interval_secs}s")
    Sys.sleep(interval_secs)
  }
}

# ---------------------------------------------------------------------------
# Download and unzip
# ---------------------------------------------------------------------------

#' Download a ZIP from `url` to `dest_dir` and extract it.
#'
#' Skips the download if the ZIP already exists in `dest_dir` (idempotent).
#'
#' @param url       Full S3 URL.
#' @param dest_dir  Local directory for the ZIP and extracted files.
#' @param n_retries Number of download retries on transient failure.
#' @return Character vector of extracted file paths.
#' @export
download_and_unzip <- function(url, dest_dir, n_retries = 3L) {
  fs::dir_create(dest_dir)
  zip_file <- file.path(dest_dir, basename(url))

  if (!fs::file_exists(zip_file)) {
    logger::log_info("Downloading {url}")
    with_retry(
      fn = function() {
        httr2::request(url) |>
          httr2::req_timeout(600) |>
          httr2::req_perform(path = zip_file)
      },
      n         = n_retries,
      error_msg = paste("download", basename(url))
    )
    logger::log_info("Saved to {zip_file}")
  } else {
    logger::log_info("ZIP already present, skipping download: {zip_file}")
  }

  extracted <- unzip(zip_file, exdir = dest_dir)
  logger::log_info("Extracted {length(extracted)} file(s) to {dest_dir}")
  extracted
}

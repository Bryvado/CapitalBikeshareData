# R/downloader.R
# URL prediction, polling, download, and unzip for Capital Bikeshare data
# ---------------------------------------------------------------------------
# Known S3 base URL
#   https://s3.amazonaws.com/capitalbikeshare-data/
#
# File-name patterns (by year):
#   2010–2017  → YYYY-capitalbikeshare-tripdata.zip    (annual)
#   2018+      → YYYYMM-capitalbikeshare-tripdata.zip  (monthly)
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
#' For 2010-2017 annual ZIPs are used; for 2018+ monthly ZIPs are used.
#' Years before 2010 are not supported.
#'
#' @param year  Integer year.
#' @param month Integer month (1-12). Ignored for 2010-2017.
#' @return A character string with the ZIP file name (no base URL).
#' @export
cbs_filename <- function(year, month = NULL) {
  year <- as.integer(year)
  if (year < 2010L) {
    stop("year must be >= 2010")
  }
  if (year >= 2010L && year <= 2017L) {
    return(sprintf("%04d-capitalbikeshare-tripdata.zip", year))
  }
  if (is.null(month)) stop("month required for year >= 2018")
  month <- as.integer(month)
  label <- sprintf("%04d%02d", year, month)
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
#' `current_year / current_month`. For 2010-2017 annual files, the next
#' year is returned with month set to 1.
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

  next_year  <- current_year
  next_month <- current_month
  if (current_year < 2010L) {
    stop("current_year must be >= 2010")
  }
  if (current_year >= 2010L && current_year <= 2017L) {
    next_year  <- current_year + 1L
    next_month <- 1L
  } else {
    # Advance by one month
    next_month <- current_month + 1L
    next_year  <- current_year
    if (next_month > 12L) {
      next_month <- 1L
      next_year  <- next_year + 1L
    }
  }

  fname <- cbs_filename(next_year, next_month)
  list(year     = next_year,
       month    = next_month,
       filename = fname,
       url      = cbs_url(fname))
}

#' List all currently available Capital Bikeshare ZIP files.
#'
#' Checks annual files for 2010-2017 and monthly files for 2018+ by probing
#' S3 URLs with HTTP HEAD requests.
#'
#' @param start_year Integer first year to check (default 2010).
#' @param end_year   Integer last year to check (default current year).
#' @return A list of named lists with fields: year, month, filename, url.
#' @export
available_cbs_files <- function(start_year = 2010L, end_year = NULL) {
  start_year <- as.integer(start_year)
  if (is.null(end_year)) {
    end_year <- as.integer(format(Sys.Date(), "%Y"))
  }
  end_year <- as.integer(end_year)

  if (start_year < 2010L) stop("start_year must be >= 2010")
  if (end_year < start_year) return(list())

  today_year  <- as.integer(format(Sys.Date(), "%Y"))
  today_month <- as.integer(format(Sys.Date(), "%m"))
  targets <- list()

  annual_end <- min(end_year, 2017L)
  if (start_year <= annual_end) {
    for (yy in start_year:annual_end) {
      fname <- cbs_filename(yy, 1L)
      url   <- cbs_url(fname)
      if (url_exists(url)) {
        targets[[length(targets) + 1L]] <- list(
          year = yy, month = 1L, filename = fname, url = url
        )
      }
    }
  }

  monthly_start <- max(start_year, 2018L)
  if (monthly_start <= end_year) {
    for (yy in monthly_start:end_year) {
      max_month <- if (yy == today_year) today_month else 12L
      for (mm in seq_len(max_month)) {
        fname <- cbs_filename(yy, mm)
        url   <- cbs_url(fname)
        if (url_exists(url)) {
          targets[[length(targets) + 1L]] <- list(
            year = yy, month = mm, filename = fname, url = url
          )
        }
      }
    }
  }

  targets
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
#' @param overwrite Logical; whether extracted files may overwrite existing files.
#' @return Character vector of extracted file paths.
#' @export
download_and_unzip <- function(url, dest_dir, n_retries = 3L, overwrite = TRUE) {
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

  extracted <- unzip(zip_file, exdir = dest_dir, overwrite = overwrite)
  logger::log_info("Extracted {length(extracted)} file(s) to {dest_dir}")
  extracted
}

#' Extract an existing local ZIP file into a destination directory.
#'
#' @param zip_file  Path to an existing local ZIP file.
#' @param dest_dir  Local directory for extracted files.
#' @param overwrite Logical; whether extracted files may overwrite existing files.
#' @return Character vector of extracted file paths.
#' @export
unzip_existing_zip <- function(zip_file, dest_dir, overwrite = TRUE) {
  if (!fs::file_exists(zip_file)) {
    stop(sprintf(
      "ZIP file does not exist: %s. Download it first with download_and_unzip().",
      zip_file
    ))
  }
  fs::dir_create(dest_dir)
  zip_listing <- unzip(zip_file, list = TRUE)
  zip_file_entries <- zip_listing$Name[!endsWith(zip_listing$Name, "/")]
  expected_file_paths <- file.path(dest_dir, zip_file_entries)
  zip_csv_entries <- zip_listing$Name[
    grepl("\\.csv$", zip_listing$Name, ignore.case = TRUE) &
      !startsWith(basename(zip_listing$Name), "._")
  ]
  expected_csv_paths <- file.path(dest_dir, zip_csv_entries)

  if (length(expected_csv_paths) > 0L &&
      all(fs::file_exists(expected_csv_paths))) {
    logger::log_info("Expected CSV files already extracted in {dest_dir} — skipping extraction")
    return(expected_file_paths[fs::file_exists(expected_file_paths)])
  }
  extracted <- unzip(zip_file, exdir = dest_dir, overwrite = overwrite)
  logger::log_info("Extracted {length(extracted)} file(s) from existing ZIP to {dest_dir}")
  expected_file_paths[fs::file_exists(expected_file_paths)]
}

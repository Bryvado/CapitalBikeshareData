# R/schema.R
# Schema standardisation and cleaning for Capital Bikeshare trip CSVs
# ---------------------------------------------------------------------------
# Two historical schemas are handled:
#
# OLD schema (pre-2018 style, no trip-level coordinates):
#   Duration, Start date, End date, Start station number,
#   Start station, End station number, End station, Bike#, Member type
#
# NEW schema (2018 onward, has trip-level coordinates):
#   ride_id, rideable_type, started_at, ended_at,
#   start_station_name, start_station_id, end_station_name, end_station_id,
#   start_lat, start_lng, end_lat, end_lng, member_casual
#
# Both are standardised to CANONICAL_COLS (defined below).
# ---------------------------------------------------------------------------

suppressPackageStartupMessages({
  library(dplyr)
  library(readr)
  library(lubridate)
  library(stringr)
  library(logger)
})

source("R/utils.R")

# ---------------------------------------------------------------------------
# Canonical output columns (shared schema for both eras)
# ---------------------------------------------------------------------------

CANONICAL_COLS <- c(
  "ride_id",           # chr  — Bike# for old, ride_id for new
  "rideable_type",     # chr  — NA for old, e.g. "electric_bike" for new
  "started_at",        # POSIXct UTC
  "ended_at",          # POSIXct UTC
  "duration_secs",     # dbl  — trip duration in seconds
  "start_station_name",# chr
  "start_station_id",  # chr  — coerced to character (old uses integers)
  "end_station_name",  # chr
  "end_station_id",    # chr
  "start_lat",         # dbl  — NA for old era (filled later by enrichment)
  "start_lng",         # dbl
  "end_lat",           # dbl
  "end_lng",           # dbl
  "user_type",         # chr  — "Member" / "Casual"
  "source_file",       # chr  — originating CSV basename
  "era"                # chr  — "old" or "new"
)

# ---------------------------------------------------------------------------
# CSV detection helpers
# ---------------------------------------------------------------------------

#' Detect whether a CSV uses the old or new schema.
#'
#' @param csv_path Path to a single CSV file.
#' @return "old" or "new".
#' @export
detect_schema <- function(csv_path) {
  header <- names(readr::read_csv(csv_path, n_max = 0L, show_col_types = FALSE))
  header_lc <- tolower(header)
  if (any(grepl("^ride_id$", header_lc))) "new" else "old"
}

# ---------------------------------------------------------------------------
# Datetime parsing (handles mixed formats)
# ---------------------------------------------------------------------------

#' Parse a character vector of dates/datetimes into POSIXct (UTC).
#'
#' Tries a sequence of common formats found in Capital Bikeshare exports.
#'
#' @param x  Character vector.
#' @return   POSIXct vector.
#' @export
parse_datetime_flex <- function(x) {
  formats <- c(
    "%Y-%m-%d %H:%M:%S",
    "%Y-%m-%dT%H:%M:%S",
    "%m/%d/%Y %H:%M",
    "%m/%d/%Y %H:%M:%S",
    "%Y-%m-%d %H:%M"
  )
  out <- rep(as.POSIXct(NA, tz = "UTC"), length(x))
  remaining <- seq_along(x)
  for (fmt in formats) {
    if (length(remaining) == 0L) break
    parsed <- as.POSIXct(x[remaining], format = fmt, tz = "UTC")
    filled <- !is.na(parsed)
    out[remaining[filled]] <- parsed[filled]
    remaining <- remaining[!filled]
  }
  if (length(remaining) > 0L) {
    logger::log_warn("{length(remaining)} datetime value(s) could not be parsed.")
  }
  out
}

# ---------------------------------------------------------------------------
# Duration parsing
# ---------------------------------------------------------------------------

#' Parse a duration field from the old schema into seconds.
#'
#' Old-schema duration is typically "H:MM:SS" or "HH:MM:SS", or raw seconds.
#'
#' @param x Character (or numeric) vector.
#' @return  Numeric vector of seconds.
#' @export
parse_duration_secs <- function(x) {
  x <- as.character(x)
  # If already numeric-looking, return as-is
  numeric_mask <- grepl("^\\d+(\\.\\d+)?$", trimws(x))
  out <- rep(NA_real_, length(x))
  out[numeric_mask] <- as.numeric(x[numeric_mask])

  hms_mask <- !numeric_mask & grepl(":", x, fixed = TRUE)
  parts <- strsplit(x[hms_mask], ":", fixed = TRUE)
  out[hms_mask] <- vapply(parts, function(p) {
    p <- suppressWarnings(as.numeric(p))
    if (length(p) == 3L && !anyNA(p)) p[1]*3600 + p[2]*60 + p[3]
    else if (length(p) == 2L && !anyNA(p)) p[1]*60 + p[2]
    else NA_real_
  }, numeric(1L))
  out
}

# ---------------------------------------------------------------------------
# Old-schema reader
# ---------------------------------------------------------------------------

#' Read and normalise an old-schema Capital Bikeshare CSV.
#'
#' @param csv_path Path to the CSV file.
#' @return A tibble with CANONICAL_COLS.
#' @export
read_old_schema <- function(csv_path) {
  raw <- readr::read_csv(
    csv_path,
    col_types  = readr::cols(.default = readr::col_character()),
    show_col_types = FALSE,
    name_repair = "minimal"
  )

  # Normalise column names: lowercase, collapse whitespace/special chars
  names(raw) <- stringr::str_to_lower(
    stringr::str_replace_all(names(raw), "[^a-zA-Z0-9]+", "_")
  )

  # Flexible column aliases for old-era files
  raw <- raw |>
    dplyr::rename_with(~ dplyr::case_when(
      .x == "start_date"          ~ "started_at",
      .x == "end_date"            ~ "ended_at",
      .x == "start_station_number"~ "start_station_id",
      .x == "end_station_number"  ~ "end_station_id",
      .x == "start_station"       ~ "start_station_name",
      .x == "end_station"         ~ "end_station_name",
      .x == "bike_"               ~ "ride_id",
      .x == "bike_number"         ~ "ride_id",
      .x == "member_type"         ~ "user_type",
      TRUE                        ~ .x
    ))

  raw |>
    dplyr::mutate(
      ride_id            = as.character(ride_id),
      rideable_type      = NA_character_,
      started_at         = parse_datetime_flex(started_at),
      ended_at           = parse_datetime_flex(ended_at),
      duration_secs      = parse_duration_secs(
        dplyr::coalesce(.data[["duration"]], NA_character_)
      ),
      start_station_id   = as.character(start_station_id),
      end_station_id     = as.character(end_station_id),
      start_lat          = NA_real_,
      start_lng          = NA_real_,
      end_lat            = NA_real_,
      end_lng            = NA_real_,
      user_type          = normalise_user_type(user_type),
      source_file        = basename(csv_path),
      era                = "old"
    ) |>
    # Derive duration from timestamps when raw duration is absent / NA
    dplyr::mutate(
      duration_secs = dplyr::if_else(
        is.na(duration_secs) & !is.na(started_at) & !is.na(ended_at),
        as.numeric(difftime(ended_at, started_at, units = "secs")),
        duration_secs
      )
    ) |>
    dplyr::select(dplyr::any_of(CANONICAL_COLS))
}

# ---------------------------------------------------------------------------
# New-schema reader
# ---------------------------------------------------------------------------

#' Read and normalise a new-schema Capital Bikeshare CSV.
#'
#' @param csv_path Path to the CSV file.
#' @return A tibble with CANONICAL_COLS.
#' @export
read_new_schema <- function(csv_path) {
  raw <- readr::read_csv(
    csv_path,
    col_types  = readr::cols(.default = readr::col_character()),
    show_col_types = FALSE,
    name_repair = "minimal"
  )

  names(raw) <- stringr::str_to_lower(
    stringr::str_replace_all(names(raw), "[^a-zA-Z0-9]+", "_")
  )

  # Alias member_casual → user_type
  if ("member_casual" %in% names(raw) && !"user_type" %in% names(raw)) {
    raw <- dplyr::rename(raw, user_type = member_casual)
  }

  raw |>
    dplyr::mutate(
      ride_id           = as.character(ride_id),
      rideable_type     = as.character(rideable_type),
      started_at        = parse_datetime_flex(started_at),
      ended_at          = parse_datetime_flex(ended_at),
      duration_secs     = as.numeric(difftime(ended_at, started_at,
                                              units = "secs")),
      start_station_id  = as.character(start_station_id),
      end_station_id    = as.character(end_station_id),
      start_lat         = suppressWarnings(as.numeric(start_lat)),
      start_lng         = suppressWarnings(as.numeric(start_lng)),
      end_lat           = suppressWarnings(as.numeric(end_lat)),
      end_lng           = suppressWarnings(as.numeric(end_lng)),
      user_type         = normalise_user_type(user_type),
      source_file       = basename(csv_path),
      era               = "new"
    ) |>
    dplyr::select(dplyr::any_of(CANONICAL_COLS))
}

# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------

#' Normalise user-type labels to "Member" or "Casual".
#'
#' @param x Character vector of raw user-type strings.
#' @return  Normalised character vector.
#' @export
normalise_user_type <- function(x) {
  x_lc <- stringr::str_to_lower(trimws(x))
  dplyr::case_when(
    x_lc %in% c("member", "subscriber") ~ "Member",
    x_lc %in% c("casual", "customer")   ~ "Casual",
    TRUE                                 ~ NA_character_
  )
}

# ---------------------------------------------------------------------------
# Top-level: read any CSV (auto-detect schema)
# ---------------------------------------------------------------------------

#' Read a Capital Bikeshare trip CSV, auto-detecting schema.
#'
#' @param csv_path Path to the CSV file.
#' @return A tibble with CANONICAL_COLS.
#' @export
read_trip_csv <- function(csv_path) {
  schema <- detect_schema(csv_path)
  logger::log_info("Reading {basename(csv_path)} as [{schema}] schema")
  fn <- if (schema == "old") read_old_schema else read_new_schema
  fn(csv_path)
}

# ---------------------------------------------------------------------------
# Validation
# ---------------------------------------------------------------------------

#' Run basic validation checks on a standardised trip tibble.
#'
#' Returns a named list with `pass` (logical) and `messages` (character).
#'
#' @param df Tibble produced by `read_trip_csv()`.
#' @export
validate_trips <- function(df) {
  msgs <- character(0L)

  if (nrow(df) == 0L)
    msgs <- c(msgs, "Empty dataset")

  if (!all(CANONICAL_COLS %in% names(df)))
    msgs <- c(msgs, paste("Missing columns:",
                          paste(setdiff(CANONICAL_COLS, names(df)),
                                collapse = ", ")))

  neg_dur <- sum(df$duration_secs < 0, na.rm = TRUE)
  if (neg_dur > 0)
    msgs <- c(msgs, sprintf("%d records with negative duration", neg_dur))

  na_start <- sum(is.na(df$started_at))
  if (na_start > 0)
    msgs <- c(msgs, sprintf("%d records with unparsed started_at", na_start))

  list(pass = length(msgs) == 0L, messages = msgs)
}

# R/enrichment.R
# Station reference enrichment: GBFS feed + coordinate fill + ride-type flags
# ---------------------------------------------------------------------------
# GBFS station_information endpoint used:
#   https://gbfs.capitalbikeshare.com/gbfs/en/station_information.json
# ---------------------------------------------------------------------------

suppressPackageStartupMessages({
  library(dplyr)
  library(jsonlite)
  library(stringdist)
  library(httr2)
  library(logger)
})

source("R/utils.R")
source("R/storage.R")

GBFS_STATION_URL <-
  "https://gbfs.capitalbikeshare.com/gbfs/en/station_information.json"

# ---------------------------------------------------------------------------
# Fetch station reference table
# ---------------------------------------------------------------------------

#' Fetch the current Capital Bikeshare station reference from the GBFS feed.
#'
#' Cache priority:
#'   1. Live GBFS network call (always attempted first).
#'   2. S3 cache (`CBS_S3_BUCKET` set and object exists) — used as fallback.
#'   3. Local file cache (`cache_path`) — used as final fallback.
#'
#' When the live call succeeds the result is written to both the local cache
#' and, in S3 mode, the S3 bucket.
#'
#' @param url   GBFS station_information URL.
#' @param cache_path  Path to a local cache CSV; re-used when the remote call
#'                    fails and no S3 cache is available.
#' @return A tibble with columns: station_id (chr), name (chr),
#'         lat (dbl), lon (dbl), capacity (int).
#' @export
fetch_station_reference <- function(
    url        = GBFS_STATION_URL,
    cache_path = file.path("data", "station_reference.csv")) {

  raw <- tryCatch(
    with_retry(
      fn = function() {
        resp <- httr2::request(url) |>
          httr2::req_timeout(30) |>
          httr2::req_perform()
        httr2::resp_body_string(resp)
      },
      n = 3, error_msg = "GBFS station_information fetch"
    ),
    error = function(e) {
      logger::log_warn("GBFS fetch failed: {conditionMessage(e)}")
      NULL
    }
  )

  if (!is.null(raw)) {
    parsed <- jsonlite::fromJSON(raw, simplifyDataFrame = TRUE)
    stations <- tibble::as_tibble(parsed$data$stations) |>
      dplyr::select(
        station_id = station_id,
        name       = name,
        lat        = lat,
        lon        = lon,
        capacity   = capacity
      ) |>
      dplyr::mutate(
        station_id = as.character(station_id),
        lat        = as.numeric(lat),
        lon        = as.numeric(lon),
        capacity   = as.integer(capacity)
      )
    # Cache locally
    fs::dir_create(dirname(cache_path))
    readr::write_csv(stations, cache_path)
    logger::log_info("Station reference refreshed: {nrow(stations)} stations")
    # Also cache in S3 when backend is configured
    if (use_s3()) {
      s3_write_csv(stations, s3_key_station_reference())
      logger::log_info("Station reference cached to S3")
    }
    return(stations)
  }

  # Fallback 1: S3 cache
  if (use_s3()) {
    key <- s3_key_station_reference()
    if (s3_object_exists(key)) {
      logger::log_warn("Using S3-cached station reference")
      return(s3_read_csv(key))
    }
  }

  # Fallback 2: local file cache
  if (file.exists(cache_path)) {
    logger::log_warn("Using cached station reference from {cache_path}")
    return(readr::read_csv(cache_path, show_col_types = FALSE))
  }

  stop("Could not obtain station reference — no live feed or cache available")
}

# ---------------------------------------------------------------------------
# Coordinate fill: exact ID match, then exact name match, then fuzzy name
# ---------------------------------------------------------------------------

#' Add coordinates to rows that are missing them by matching against the
#' station reference table.
#'
#' Matching strategy (applied independently for start and end):
#'   1. Exact match on station_id.
#'   2. Exact match on station_name (case-insensitive, trimmed).
#'   3. Fuzzy match on station_name (Jaro-Winkler; only when
#'      similarity ≥ `fuzzy_threshold`).
#'
#' @param df              Standardised trip tibble.
#' @param station_ref     Station reference tibble from `fetch_station_reference()`.
#' @param fuzzy_threshold Minimum Jaro-Winkler similarity for fuzzy match (0–1).
#' @return The tibble with start_lat/lng and end_lat/lng filled where possible.
#' @export
fill_missing_coordinates <- function(df,
                                     station_ref,
                                     fuzzy_threshold = 0.92) {
  df <- fill_coords_for_endpoint(df, station_ref,
                                 id_col   = "start_station_id",
                                 name_col = "start_station_name",
                                 lat_col  = "start_lat",
                                 lng_col  = "start_lng",
                                 fuzzy_threshold = fuzzy_threshold)

  df <- fill_coords_for_endpoint(df, station_ref,
                                 id_col   = "end_station_id",
                                 name_col = "end_station_name",
                                 lat_col  = "end_lat",
                                 lng_col  = "end_lng",
                                 fuzzy_threshold = fuzzy_threshold)
  df
}

# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

#' Fill lat/lng for one endpoint (start or end) using three-stage matching.
#'
#' @keywords internal
fill_coords_for_endpoint <- function(df, station_ref,
                                     id_col, name_col, lat_col, lng_col,
                                     fuzzy_threshold) {
  need_fill <- is.na(df[[lat_col]]) | is.na(df[[lng_col]])
  if (!any(need_fill)) return(df)

  # --- Stage 1: exact ID match ---
  ref_id <- station_ref |>
    dplyr::select(station_id, ref_lat = lat, ref_lon = lon)

  df <- df |>
    dplyr::left_join(ref_id,
                     by = stats::setNames("station_id", id_col),
                     relationship = "many-to-one") |>
    dplyr::mutate(
      !!lat_col := dplyr::if_else(need_fill & !is.na(ref_lat),
                                  ref_lat, .data[[lat_col]]),
      !!lng_col := dplyr::if_else(need_fill & !is.na(ref_lon),
                                  ref_lon, .data[[lng_col]])
    ) |>
    dplyr::select(-ref_lat, -ref_lon)

  # Update which rows still need filling
  need_fill <- is.na(df[[lat_col]]) | is.na(df[[lng_col]])
  if (!any(need_fill)) return(df)

  # --- Stage 2: exact name match (case-insensitive) ---
  ref_name <- station_ref |>
    dplyr::mutate(name_lc = tolower(trimws(name))) |>
    dplyr::select(name_lc, ref_lat = lat, ref_lon = lon)

  df <- df |>
    dplyr::mutate(name_lc_tmp_ = tolower(trimws(.data[[name_col]]))) |>
    dplyr::left_join(ref_name,
                     by = c("name_lc_tmp_" = "name_lc"),
                     relationship = "many-to-one") |>
    dplyr::mutate(
      !!lat_col := dplyr::if_else(need_fill & !is.na(ref_lat),
                                  ref_lat, .data[[lat_col]]),
      !!lng_col := dplyr::if_else(need_fill & !is.na(ref_lon),
                                  ref_lon, .data[[lng_col]])
    ) |>
    dplyr::select(-ref_lat, -ref_lon, -name_lc_tmp_)

  # Update which rows still need filling
  need_fill <- is.na(df[[lat_col]]) | is.na(df[[lng_col]])
  if (!any(need_fill)) return(df)

  # --- Stage 3: fuzzy name match (Jaro-Winkler) ---
  unique_missing_names <- unique(df[[name_col]][need_fill])
  unique_missing_names <- unique_missing_names[!is.na(unique_missing_names)]

  if (length(unique_missing_names) == 0L) return(df)

  ref_names_vec <- station_ref$name
  fuzzy_map <- lapply(unique_missing_names, function(q) {
    sims <- 1 - stringdist::stringdist(
      tolower(q), tolower(ref_names_vec), method = "jw"
    )
    best_idx <- which.max(sims)
    if (sims[best_idx] >= fuzzy_threshold) {
      data.frame(
        query    = q,
        ref_lat  = station_ref$lat[best_idx],
        ref_lon  = station_ref$lon[best_idx],
        stringsAsFactors = FALSE
      )
    } else {
      NULL
    }
  })
  fuzzy_df <- do.call(rbind, fuzzy_map)

  if (!is.null(fuzzy_df) && nrow(fuzzy_df) > 0L) {
    df <- df |>
      dplyr::left_join(fuzzy_df,
                       by = stats::setNames("query", name_col),
                       relationship = "many-to-one") |>
      dplyr::mutate(
        !!lat_col := dplyr::if_else(need_fill & !is.na(ref_lat),
                                    ref_lat, .data[[lat_col]]),
        !!lng_col := dplyr::if_else(need_fill & !is.na(ref_lon),
                                    ref_lon, .data[[lng_col]])
      ) |>
      dplyr::select(-ref_lat, -ref_lon)
  }

  df
}

# ---------------------------------------------------------------------------
# Station-based flags and ride_type
# ---------------------------------------------------------------------------

#' Add station-based flags and a ride_type classification to a trip tibble.
#'
#' Station-based = the trip endpoint has a valid station name and ID.
#'
#' @param df Standardised (and coordinate-filled) trip tibble.
#' @return   Tibble with additional columns:
#'           station_based_start, station_based_end,
#'           non_station_start, non_station_end, ride_type.
#' @export
add_ride_type_flags <- function(df) {
  df |>
    dplyr::mutate(
      station_based_start = !is.na(start_station_id) &
                            start_station_id != "" &
                            !is.na(start_station_name),
      station_based_end   = !is.na(end_station_id)   &
                            end_station_id != ""   &
                            !is.na(end_station_name),
      non_station_start   = !station_based_start,
      non_station_end     = !station_based_end,
      ride_type           = dplyr::case_when(
        station_based_start & station_based_end  ~ "station-to-station",
        !station_based_start & station_based_end ~ "point-to-station",
        station_based_start & !station_based_end ~ "station-to-point",
        TRUE                                     ~ "point-to-point"
      )
    )
}

# ---------------------------------------------------------------------------
# Top-level enrichment call
# ---------------------------------------------------------------------------

#' Enrich a standardised trip tibble with station coordinates and ride-type.
#'
#' @param df             Standardised trip tibble.
#' @param station_ref    Result of `fetch_station_reference()`.
#' @param fuzzy_threshold Jaro-Winkler threshold for fuzzy name matching.
#' @return Enriched tibble.
#' @export
enrich_trips <- function(df, station_ref, fuzzy_threshold = 0.92) {
  n_before <- sum(is.na(df$start_lat) | is.na(df$end_lat))
  df <- fill_missing_coordinates(df, station_ref, fuzzy_threshold)
  n_after  <- sum(is.na(df$start_lat) | is.na(df$end_lat))
  logger::log_info("Coordinate fill: {n_before - n_after} rows filled ",
                   "({n_after} still missing)")
  add_ride_type_flags(df)
}

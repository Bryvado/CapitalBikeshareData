# R/analysis.R
# Data-availability analysis and census-tract trip-density visualisation
# ---------------------------------------------------------------------------
# Requires the pipeline to have been run (manifest.csv + master parquet files).
#
# Quick start:
#   source("R/analysis.R")
#   results <- run_analysis()           # writes PNGs to data/plots/
#   results$availability                # tibble: year, month, n_trips, era
#   results$tracts                      # sf object: census tracts
#   results$plots$availability_chart
#   results$plots$tract_map
# ---------------------------------------------------------------------------

suppressPackageStartupMessages({
  library(arrow)
  library(dplyr)
  library(readr)
  library(lubridate)
  library(ggplot2)
  library(sf)
  library(tigris)
  library(fs)
  library(logger)
})

source("R/utils.R")
source("R/storage.R")
source("R/parquet.R")

# ---------------------------------------------------------------------------
# 1. Data availability summary
# ---------------------------------------------------------------------------

#' Summarise which year-months of data are available and how many trips each
#' period contains.
#'
#' Reads the master parquet files (both eras) and the manifest.  Returns a
#' tidy tibble with one row per year-month period.
#'
#' @param root  Project root directory.
#' @return A tibble with columns: year, month, period (label), era, n_trips,
#'         processed_at, status.
#' @export
summarize_data_availability <- function(root = ".") {
  logger::log_info("Reading manifest…")
  mf <- read_manifest(manifest_path(root))

  if (nrow(mf) == 0L) {
    logger::log_warn("Manifest is empty — has the pipeline been run?")
    return(tibble::tibble(
      year = integer(), month = integer(), period = character(),
      era = character(), n_trips = integer(),
      processed_at = as.POSIXct(character()), status = character()
    ))
  }

  avail <- mf |>
    dplyr::filter(status == "ok") |>
    dplyr::mutate(
      # Derive year / month from filename (YYYY- or YYYYMM- prefix)
      period = sub("-capitalbikeshare-tripdata\\.zip$", "", basename(filename)),
      year   = dplyr::case_when(
        nchar(period) == 4L ~ as.integer(period),          # "YYYY"
        nchar(period) == 6L ~ as.integer(substr(period, 1, 4)), # "YYYYMM"
        TRUE                ~ NA_integer_
      ),
      month  = dplyr::case_when(
        nchar(period) == 4L ~ 1L,
        nchar(period) == 6L ~ as.integer(substr(period, 5, 6)),
        TRUE                ~ NA_integer_
      ),
      n_trips      = as.integer(rows),
      processed_at = as.POSIXct(processed_at)
    ) |>
    dplyr::select(year, month, period, era, n_trips, processed_at, status) |>
    dplyr::arrange(year, month)

  logger::log_info(
    "Data availability: {nrow(avail)} period(s) from {min(avail$year)} to {max(avail$year)}"
  )
  avail
}

# ---------------------------------------------------------------------------
# 2. Census tract download via tigris
# ---------------------------------------------------------------------------

#' Determine the geographic bounding box of all bikeshare trip coordinates.
#'
#' Reads both master parquets and returns a named list with
#' xmin, xmax (longitude) and ymin, ymax (latitude).
#'
#' @param root    Project root directory.
#' @param sample  Maximum rows to use per era when computing extent
#'                (NULL = all rows).
#' @return A named list: list(xmin, xmax, ymin, ymax).
#' @export
trip_extent <- function(root = ".", sample = 500000L) {
  lats <- numeric(0L)
  lngs <- numeric(0L)

  for (era in c("old", "new")) {
    mp <- master_path(era, root)
    if (!fs::file_exists(mp)) next
    df <- arrow::read_parquet(mp,
                              col_select = c("start_lat", "start_lng",
                                             "end_lat",   "end_lng"))
    if (!is.null(sample) && nrow(df) > sample) {
      df <- df[sample(nrow(df), sample), ]
    }
    lats <- c(lats, df$start_lat, df$end_lat)
    lngs <- c(lngs, df$start_lng, df$end_lng)
  }

  lats <- lats[!is.na(lats)]
  lngs <- lngs[!is.na(lngs)]

  if (length(lats) == 0L) stop("No coordinate data found in master parquets.")

  # Clip to plausible Capital Bikeshare service area before taking extent
  lat_range <- c(38.7, 39.2)
  lng_range <- c(-77.6, -76.8)
  lats <- lats[lats >= lat_range[1] & lats <= lat_range[2]]
  lngs <- lngs[lngs >= lng_range[1] & lngs <= lng_range[2]]

  list(
    xmin = min(lngs), xmax = max(lngs),
    ymin = min(lats), ymax = max(lats)
  )
}

#' Download census tracts covering the bikeshare service area using tigris.
#'
#' Identifies which states overlap the trip bounding box (DC / MD / VA) and
#' fetches tract-level shapefiles via `tigris::tracts()`.  The returned sf
#' object is clipped to the bounding box so only relevant tracts are kept.
#'
#' @param extent  Named list with xmin/xmax/ymin/ymax (from `trip_extent()`).
#'                If NULL, a default Capital Bikeshare bounding box is used.
#' @param year    Decennial census year for the tract boundaries (default 2020).
#' @param cache   Whether to cache tigris downloads (default TRUE).
#' @return An `sf` object of census tracts.
#' @export
download_census_tracts <- function(extent = NULL, year = 2020L, cache = TRUE) {
  tigris::options(use_tigris_cache = cache)

  if (is.null(extent)) {
    extent <- list(xmin = -77.6, xmax = -76.8, ymin = 38.7, ymax = 39.2)
  }

  # States whose territory intersects the Capital Bikeshare service area
  service_states <- list(
    list(fips = "11", name = "DC"),   # Washington, D.C.
    list(fips = "24", name = "MD"),   # Maryland
    list(fips = "51", name = "VA")    # Virginia
  )

  bbox_poly <- sf::st_as_sfc(
    sf::st_bbox(c(xmin = extent$xmin, xmax = extent$xmax,
                  ymin = extent$ymin, ymax = extent$ymax),
                crs = 4326L)
  )

  tract_list <- lapply(service_states, function(s) {
    logger::log_info("Downloading census tracts for {s$name} (FIPS {s$fips}), year {year}…")
    tracts_sf <- tigris::tracts(state = s$fips, year = year, progress_bar = FALSE)
    tracts_sf <- sf::st_transform(tracts_sf, 4326L)
    tracts_sf[sf::st_intersects(tracts_sf, bbox_poly, sparse = FALSE)[, 1], ]
  })

  tracts_sf <- do.call(rbind, tract_list)
  logger::log_info("Downloaded {nrow(tracts_sf)} census tracts covering the service area")
  tracts_sf
}

# ---------------------------------------------------------------------------
# 3. Aggregate trip starts to census tracts
# ---------------------------------------------------------------------------

#' Count bikeshare trip starts per census tract.
#'
#' Spatially joins the trip start coordinates to the tract polygons.
#'
#' @param root         Project root directory.
#' @param tracts_sf    sf object of census tracts (from `download_census_tracts()`).
#' @param year_filter  Integer vector of years to include (NULL = all years).
#' @param sample       Maximum rows per era to use (NULL = all rows; useful
#'                     when memory is limited).
#' @return The `tracts_sf` object with an added `n_trips` column.
#' @export
aggregate_trips_to_tracts <- function(root = ".", tracts_sf,
                                      year_filter = NULL, sample = NULL) {
  coord_rows <- list()

  for (era in c("old", "new")) {
    mp <- master_path(era, root)
    if (!fs::file_exists(mp)) next
    df <- arrow::read_parquet(
      mp,
      col_select = c("started_at", "start_lat", "start_lng")
    )
    df <- df |>
      dplyr::filter(!is.na(start_lat), !is.na(start_lng)) |>
      dplyr::mutate(year = lubridate::year(started_at))

    if (!is.null(year_filter)) {
      df <- dplyr::filter(df, year %in% year_filter)
    }

    if (!is.null(sample) && nrow(df) > sample) {
      df <- df[sample(nrow(df), sample), ]
    }
    coord_rows[[era]] <- df
  }

  if (length(coord_rows) == 0L) stop("No trip coordinate data found.")

  trips_df <- dplyr::bind_rows(coord_rows)

  trips_sf <- sf::st_as_sf(
    trips_df,
    coords = c("start_lng", "start_lat"),
    crs    = 4326L,
    remove = FALSE
  )
  tracts_proj <- sf::st_transform(tracts_sf, sf::st_crs(trips_sf))

  logger::log_info("Spatially joining {nrow(trips_sf)} trip starts to {nrow(tracts_proj)} tracts…")
  joined <- sf::st_join(trips_sf, tracts_proj["GEOID"], left = FALSE)

  counts <- joined |>
    sf::st_drop_geometry() |>
    dplyr::count(GEOID, name = "n_trips")

  tracts_out <- tracts_sf |>
    dplyr::left_join(counts, by = "GEOID") |>
    dplyr::mutate(n_trips = tidyr::replace_na(n_trips, 0L))

  logger::log_info("Trip aggregation complete.")
  tracts_out
}

# ---------------------------------------------------------------------------
# 4. Plots
# ---------------------------------------------------------------------------

#' Build a year × month availability heatmap.
#'
#' @param avail  Tibble from `summarize_data_availability()`.
#' @return A ggplot2 object.
#' @export
build_availability_chart <- function(avail) {
  if (nrow(avail) == 0L) {
    stop("No availability data to plot — run the pipeline first.")
  }

  month_labels <- c("Jan", "Feb", "Mar", "Apr", "May", "Jun",
                    "Jul", "Aug", "Sep", "Oct", "Nov", "Dec")

  plot_df <- avail |>
    dplyr::mutate(
      month_fac = factor(month, levels = 1:12, labels = month_labels),
      year_fac  = factor(year)
    )

  ggplot2::ggplot(
    plot_df,
    ggplot2::aes(x = month_fac, y = year_fac, fill = n_trips)
  ) +
    ggplot2::geom_tile(color = "white", linewidth = 0.4) +
    ggplot2::scale_fill_viridis_c(
      name   = "Trips",
      labels = scales::comma,
      option = "plasma",
      na.value = "grey85"
    ) +
    ggplot2::labs(
      title    = "Capital Bikeshare — Data Availability by Year & Month",
      subtitle = paste0(
        "Shaded cells = successfully processed files; colour = trip count.\n",
        "Annual (2010-2017) files shown in January column."
      ),
      x = "Month",
      y = "Year"
    ) +
    ggplot2::theme_minimal(base_size = 12) +
    ggplot2::theme(
      panel.grid  = ggplot2::element_blank(),
      axis.text.x = ggplot2::element_text(angle = 45, hjust = 1)
    )
}

#' Build a choropleth map of trip starts per census tract.
#'
#' @param tracts_sf   sf object with `n_trips` column (from
#'                    `aggregate_trips_to_tracts()`).
#' @param title       Plot title string.
#' @return A ggplot2 object.
#' @export
build_tract_map <- function(tracts_sf,
                            title = "Capital Bikeshare Trip Starts by Census Tract") {
  ggplot2::ggplot(tracts_sf) +
    ggplot2::geom_sf(
      ggplot2::aes(fill = n_trips),
      color = "white", linewidth = 0.1
    ) +
    ggplot2::scale_fill_viridis_c(
      name      = "Trip starts",
      labels    = scales::comma,
      option    = "inferno",
      trans     = "log1p",
      na.value  = "grey85",
      breaks    = c(0, 100, 1000, 10000, 100000),
      guide     = ggplot2::guide_colorbar(barheight = 10)
    ) +
    ggplot2::labs(
      title    = title,
      subtitle = "Log-scaled colour; Census Tract boundaries via tigris",
      caption  = "Source: Capital Bikeshare open data · tigris / US Census Bureau"
    ) +
    ggplot2::coord_sf(crs = 4326L) +
    ggplot2::theme_void(base_size = 12) +
    ggplot2::theme(
      plot.title    = ggplot2::element_text(face = "bold", size = 14),
      plot.subtitle = ggplot2::element_text(size = 10, color = "grey40"),
      plot.caption  = ggplot2::element_text(size = 8,  color = "grey60"),
      legend.position = "right"
    )
}

# ---------------------------------------------------------------------------
# 5. Top-level orchestrator
# ---------------------------------------------------------------------------

#' Run the full analysis pipeline.
#'
#' Steps:
#'   1. Summarise data availability from the manifest / parquet files.
#'   2. Derive the geographic extent of the trip coordinates.
#'   3. Download census tracts via `tigris` for that extent.
#'   4. Aggregate trip starts to tracts.
#'   5. Build and save all plots.
#'
#' @param root         Project root directory.
#' @param tract_year   Decennial year for tigris tract boundaries (default 2020).
#' @param year_filter  Integer vector of years to map.  NULL = all years.
#' @param plots_dir    Directory for saved PNG plots.
#' @param sample       Max rows per era when computing extent / aggregating.
#'                     Reduce if memory-constrained; NULL = use all rows.
#' @return Invisibly, a named list:
#'   \describe{
#'     \item{availability}{Tibble with year/month/n_trips coverage.}
#'     \item{tracts}{sf object of downloaded tracts with n_trips.}
#'     \item{plots}{Named list of ggplot2 objects.}
#'   }
#' @export
run_analysis <- function(root         = ".",
                         tract_year   = 2020L,
                         year_filter  = NULL,
                         plots_dir    = file.path(root, "data", "plots"),
                         sample       = 1000000L) {

  init_logger(log_dir = file.path(root, "logs"))
  logger::log_info("=== Capital Bikeshare Analysis START ===")

  fs::dir_create(plots_dir)

  # --- Step 1: availability summary ---
  avail <- summarize_data_availability(root)
  logger::log_info(
    "Coverage: {nrow(avail)} month(s), {sum(avail$n_trips, na.rm=TRUE)} total trips"
  )

  avail_chart <- build_availability_chart(avail)
  avail_path  <- file.path(plots_dir, "availability_heatmap.png")
  ggplot2::ggsave(avail_path, avail_chart, width = 12, height = 7, dpi = 150)
  logger::log_info("Saved availability heatmap to {avail_path}")

  # --- Step 2 & 3: extent + tract download ---
  logger::log_info("Computing trip coordinate extent…")
  ext <- tryCatch(
    trip_extent(root = root, sample = sample),
    error = function(e) {
      logger::log_warn("Could not compute extent from data: {conditionMessage(e)}. ",
                       "Falling back to default Capital Bikeshare service area.")
      list(xmin = -77.6, xmax = -76.8, ymin = 38.7, ymax = 39.2)
    }
  )
  logger::log_info(
    "Extent: lng [{round(ext$xmin,4)}, {round(ext$xmax,4)}], ",
    "lat [{round(ext$ymin,4)}, {round(ext$ymax,4)}]"
  )

  tracts_sf <- download_census_tracts(extent = ext, year = tract_year)

  # --- Step 4: aggregate trips to tracts ---
  tracts_with_trips <- tryCatch(
    aggregate_trips_to_tracts(root       = root,
                              tracts_sf  = tracts_sf,
                              year_filter = year_filter,
                              sample     = sample),
    error = function(e) {
      logger::log_warn("Trip aggregation failed: {conditionMessage(e)}. ",
                       "Returning tracts without trip counts.")
      tracts_sf$n_trips <- NA_integer_
      tracts_sf
    }
  )

  # --- Step 5: map ---
  map_title <- if (!is.null(year_filter)) {
    paste0("Capital Bikeshare Trip Starts by Census Tract (",
           paste(sort(year_filter), collapse = ", "), ")")
  } else {
    "Capital Bikeshare Trip Starts by Census Tract (All Years)"
  }

  tract_map  <- build_tract_map(tracts_with_trips, title = map_title)
  map_path   <- file.path(plots_dir, "tract_trip_density_map.png")
  ggplot2::ggsave(map_path, tract_map, width = 10, height = 10, dpi = 150)
  logger::log_info("Saved tract map to {map_path}")

  logger::log_info("=== Capital Bikeshare Analysis COMPLETE ===")

  invisible(list(
    availability = avail,
    tracts       = tracts_with_trips,
    plots        = list(
      availability_chart = avail_chart,
      tract_map          = tract_map
    )
  ))
}

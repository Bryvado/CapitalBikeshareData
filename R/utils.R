# R/utils.R
# Shared utilities: logging, retry logic, path helpers
# ---------------------------------------------------------------------------

suppressPackageStartupMessages({
  library(logger)
  library(fs)
})

# ---------------------------------------------------------------------------
# Logging setup
# ---------------------------------------------------------------------------

#' Initialise the pipeline logger.
#'
#' @param log_dir Directory for log files (created if absent).
#' @param log_file Filename inside log_dir.
#' @param level   logger threshold (e.g. logger::TRACE, logger::INFO).
#' @export
init_logger <- function(log_dir  = "logs",
                        log_file = "pipeline.log",
                        level    = logger::INFO) {
  fs::dir_create(log_dir)
  log_path <- file.path(log_dir, log_file)

  logger::log_appender(logger::appender_tee(log_path), index = 1)
  logger::log_threshold(level, index = 1)
  logger::log_formatter(logger::formatter_glue_or_sprintf, index = 1)
  logger::log_layout(logger::layout_glue_colors, index = 1)
  invisible(log_path)
}

# ---------------------------------------------------------------------------
# Retry logic
# ---------------------------------------------------------------------------

#' Retry a function call up to `n` times with exponential back-off.
#'
#' @param fn          A zero-argument function to attempt.
#' @param n           Maximum number of attempts.
#' @param wait_base   Base wait time in seconds (doubles each retry).
#' @param error_msg   Label used in log messages.
#' @return The return value of `fn` on success.
#' @export
with_retry <- function(fn, n = 3, wait_base = 5, error_msg = "operation") {
  attempt <- 0L
  repeat {
    attempt <- attempt + 1L
    result <- tryCatch(fn(), error = function(e) e)
    if (!inherits(result, "error")) return(result)
    if (attempt >= n) stop(sprintf("%s failed after %d attempts: %s",
                                   error_msg, n, conditionMessage(result)))
    wait <- wait_base * 2^(attempt - 1L)
    logger::log_warn("Attempt {attempt}/{n} for [{error_msg}] failed — ",
                     "retrying in {wait}s: {conditionMessage(result)}")
    Sys.sleep(wait)
  }
}

# ---------------------------------------------------------------------------
# Path helpers
# ---------------------------------------------------------------------------

#' Canonical raw-data directory for a given period label.
#'
#' @param label  Period label, e.g. "202405" or "2014".
#' @param root   Project root (default: working directory).
#' @export
raw_dir <- function(label, root = ".") {
  file.path(root, "data", "raw", label)
}

#' Canonical processed-data directory for a given period label.
#'
#' @param label  Period label, e.g. "202405".
#' @param root   Project root.
#' @export
processed_dir <- function(label, root = ".") {
  file.path(root, "data", "processed", label)
}

#' Path to a master parquet dataset.
#'
#' @param era  "old" (no native coordinates) or "new" (has coordinates).
#' @param root Project root.
#' @export
master_path <- function(era = c("new", "old"), root = ".") {
  era <- match.arg(era)
  file.path(root, "data", "master", paste0(era, "_era.parquet"))
}

#' Path to the processing manifest CSV.
#'
#' @param root Project root.
#' @export
manifest_path <- function(root = ".") {
  file.path(root, "manifest.csv")
}

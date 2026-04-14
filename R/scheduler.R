# R/scheduler.R
# Monthly polling job scheduler for the Capital Bikeshare pipeline.
# ---------------------------------------------------------------------------
# Two scheduling approaches are provided:
#
#   A) cronR  — cron-based scheduler for Linux / macOS servers.
#      Schedules a system cron job that runs the pipeline on the 1st of each
#      month at midnight, then polls every 15 minutes until the file appears.
#
#   B) taskscheduleR — Windows Task Scheduler integration.
#
# Both approaches install a small R driver script (`run_cbs_pipeline.R`)
# that invokes `run_pipeline()`.  The polling loop is already built into
# `poll_until_available()`, so the cron job only needs to be triggered once
# per month (on the 1st).
# ---------------------------------------------------------------------------

suppressPackageStartupMessages({
  library(logger)
  library(fs)
})

source("R/utils.R")

# ---------------------------------------------------------------------------
# Shared: write the driver script
# ---------------------------------------------------------------------------

#' Write (or overwrite) the monthly driver script that cron/taskscheduleR
#' will invoke.
#'
#' @param script_path  Absolute path where the driver R script will be saved.
#' @param project_root Absolute path to the project root passed to
#'                     `run_pipeline()`.
#' @param poll_interval_secs Seconds between S3 polls (default 900 = 15 min).
#' @param poll_timeout_secs  Max seconds to wait (default 30 days).
#' @export
write_driver_script <- function(
    script_path         = fs::path_abs("run_cbs_pipeline.R"),
    project_root        = fs::path_abs("."),
    poll_interval_secs  = 900L,
    poll_timeout_secs   = 30L * 24L * 3600L) {

  script_body <- sprintf(
'# Auto-generated driver — do not edit manually.
# Invoked by the scheduler on the 1st of every month.
setwd("%s")
source("R/pipeline.R")
run_pipeline(
  root          = "%s",
  poll_interval = %dL,
  poll_timeout  = %dL
)
',
    project_root, project_root,
    as.integer(poll_interval_secs),
    as.integer(poll_timeout_secs)
  )

  writeLines(script_body, script_path)
  logger::log_info("Driver script written to {script_path}")
  invisible(script_path)
}

# ---------------------------------------------------------------------------
# Approach A: cronR (Linux / macOS)
# ---------------------------------------------------------------------------

#' Schedule the pipeline using cronR on Linux / macOS.
#'
#' Installs a cron job that runs on the 1st of every month at 00:01 AM.
#' Requires the `cronR` package.
#'
#' @param rscript_path    Path to the Rscript binary (auto-detected if NULL).
#' @param script_path     Path to the driver R script (created if absent).
#' @param project_root    Project root directory.
#' @param log_file        File where cron stdout/stderr are redirected.
#' @param poll_interval_secs Seconds between S3 polls.
#' @param poll_timeout_secs  Max seconds to wait for the ZIP.
#' @export
schedule_with_cronr <- function(
    rscript_path        = NULL,
    script_path         = fs::path_abs("run_cbs_pipeline.R"),
    project_root        = fs::path_abs("."),
    log_file            = fs::path_abs(file.path("logs", "cron_pipeline.log")),
    poll_interval_secs  = 900L,
    poll_timeout_secs   = 30L * 24L * 3600L) {

  if (!requireNamespace("cronR", quietly = TRUE))
    stop("Package 'cronR' is required. Install with: install.packages('cronR')")

  if (is.null(rscript_path))
    rscript_path <- file.path(R.home("bin"), "Rscript")

  write_driver_script(script_path, project_root,
                      poll_interval_secs, poll_timeout_secs)

  # Build the cron command
  cmd <- cronR::cron_rscript(
    rscript      = rscript_path,
    rscript_args = script_path,
    log_append   = TRUE,
    log_file     = log_file
  )

  # Run at 00:01 AM on the 1st of every month
  cronR::cron_add(
    command  = cmd,
    frequency = "monthly",
    at        = "00:01",
    id        = "cbs_pipeline",
    description = "Capital Bikeshare monthly data pipeline"
  )

  logger::log_info("cronR job installed: pipeline will run on the 1st of each month at 00:01")
  invisible(cmd)
}

#' Remove the cronR job for the Capital Bikeshare pipeline.
#' @export
unschedule_with_cronr <- function() {
  if (!requireNamespace("cronR", quietly = TRUE))
    stop("Package 'cronR' is required.")
  cronR::cron_rm("cbs_pipeline")
  logger::log_info("cronR job removed")
}

# ---------------------------------------------------------------------------
# Approach B: taskscheduleR (Windows)
# ---------------------------------------------------------------------------

#' Schedule the pipeline using taskscheduleR on Windows.
#'
#' Installs a Windows Task Scheduler task that runs on the 1st of each month.
#' Requires the `taskscheduleR` package.
#'
#' @param rscript_path    Path to the Rscript.exe binary (auto-detected if NULL).
#' @param script_path     Path to the driver R script (created if absent).
#' @param project_root    Project root directory.
#' @param poll_interval_secs Seconds between S3 polls.
#' @param poll_timeout_secs  Max seconds to wait for the ZIP.
#' @export
schedule_with_taskscheduler <- function(
    rscript_path       = NULL,
    script_path        = fs::path_abs("run_cbs_pipeline.R"),
    project_root       = fs::path_abs("."),
    poll_interval_secs = 900L,
    poll_timeout_secs  = 30L * 24L * 3600L) {

  if (!requireNamespace("taskscheduleR", quietly = TRUE))
    stop("Package 'taskscheduleR' is required. Install with: install.packages('taskscheduleR')")

  if (is.null(rscript_path))
    rscript_path <- file.path(R.home("bin"), "Rscript.exe")

  write_driver_script(script_path, project_root,
                      poll_interval_secs, poll_timeout_secs)

  taskscheduleR::taskscheduler_create(
    taskname = "CBSPipeline",
    rscript  = script_path,
    schedule = "MONTHLY",
    starttime = "00:01",
    startdate = format(Sys.Date(), "%d/%m/%Y"),
    Rexe     = rscript_path
  )

  logger::log_info("Windows Task Scheduler job 'CBSPipeline' installed")
  invisible(TRUE)
}

#' Remove the Windows Task Scheduler job for the Capital Bikeshare pipeline.
#' @export
unschedule_with_taskscheduler <- function() {
  if (!requireNamespace("taskscheduleR", quietly = TRUE))
    stop("Package 'taskscheduleR' is required.")
  taskscheduleR::taskscheduler_delete("CBSPipeline")
  logger::log_info("Windows Task Scheduler job 'CBSPipeline' removed")
}

# ---------------------------------------------------------------------------
# Convenience: show current schedule status
# ---------------------------------------------------------------------------

#' Print a summary of currently installed Capital Bikeshare cron jobs.
#' Requires cronR (Linux/macOS).
#' @export
show_schedule <- function() {
  if (!requireNamespace("cronR", quietly = TRUE)) {
    message("cronR not installed; check system crontab manually.")
    return(invisible(NULL))
  }
  jobs <- cronR::cron_ls()
  cbs  <- jobs[grepl("cbs_pipeline", jobs, fixed = TRUE)]
  if (length(cbs) == 0L) message("No Capital Bikeshare cron job found.")
  else cat(cbs, sep = "\n")
  invisible(cbs)
}

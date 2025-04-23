#' Log a batch process result to the summary table
#'
#' @description
#' Appends a new log entry (OK or ERROR) to the summary tibble.
#'
#' @param summary_table A tibble created by build_summary_table()
#' @param user_login Character. User or system running the pipeline
#' @param batch_id Integer. Identifier for the current batch
#' @param symbol Character. Stock symbol being processed
#' @param status Character. Status of the operation ("OK" or "ERROR")
#' @param n_rows Integer. Number of rows processed in this batch
#' @param message Character. Additional information or error message
#'
#' @return The updated summary_table tibble
#'
#' @importFrom tibble add_row
#' @export
log_summary <- function(summary_table,
                        user_login,
                        batch_id,
                        symbol,
                        status = c("OK", "ERROR"),
                        n_rows = 0,
                        message = "") {
  status <- match.arg(status)
  timestamp <- Sys.time()
  tibble::add_row(
    summary_table,
    user_login = as.character(user_login),
    batch_id = as.integer(batch_id),
    symbol = as.character(symbol),
    status = status,
    n_rows = as.integer(n_rows),
    message = as.character(message),
    timestamp = timestamp
  )
}

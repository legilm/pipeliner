#' Push the summary log table to the database
#'
#' @description
#' Inserts all rows from the summary log tibble into the pipeline_logs table in the database.
#'
#' @param conn A DBI connection object to the database
#' @param summary_table A tibble created by build_summary_table() and filled by log_summary()
#'
#' @return Number of rows inserted
#'
#' @importFrom DBI dbWriteTable
#' @export
push_summary_table <- function(conn, summary_table) {
  if (nrow(summary_table) == 0) {
    message("No logs to push.")
    return(0)
  }
  DBI::dbWriteTable(
    conn,
    name = "pipeline_logs",
    value = summary_table,
    append = TRUE,
    row.names = FALSE
  )
  message(sprintf("Inserted %d log rows into pipeline_logs.", nrow(summary_table)))
  return(nrow(summary_table))
}

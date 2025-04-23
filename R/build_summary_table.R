#' Build an empty summary log table
#'
#' @description
#' Creates an empty tibble to store batch process logs, matching the pipeline_logs schema.
#'
#' @return A tibble with columns: id, user_login, batch_id, symbol, status, n_rows, message, timestamp
#'
#' @importFrom tibble tibble
#' @export
build_summary_table <- function() {
  tibble::tibble(
    # id = integer(),           # PK, will be auto-filled by the database
    user_login = character(),
    batch_id = integer(),
    symbol = character(),
    status = character(),
    n_rows = integer(),
    message = character(),
    timestamp = as.POSIXct(character())
  )
}

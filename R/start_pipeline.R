#' Start Data Pipeline
#'
#' @export
start_pipeline <- function() {
  tryCatch({
    conn <- connect_db()
    on.exit(DBI::dbDisconnect(conn), add = TRUE)

    symbols <- fetch_symbols(conn)
    message(sprintf("Found %d symbols to process", length(symbols)))

    long_data <- split_batch(
      symbols = symbols,
      batch_size = 1,
      sleep_time = 2
    )

    if (!is.null(long_data) && nrow(long_data) > 0) {
      rows <- insert_new_data(conn, long_data)
      message(sprintf("Successfully inserted %d new rows", rows))
    } else {
      message("No data to insert")
    }

  }, error = function(e) {
    message("Error in pipeline: ", e$message)
    return(NULL)
  })
}

#' Start Data Pipeline
#'
#' @export
start_pipeline <- function(user_login = Sys.info()[["user"]]) {
  # Initialize summary table
  summary_table <- build_summary_table()

  tryCatch({
    conn <- connect_db()
    on.exit({
      push_summary_table(conn, summary_table)
      DBI::dbDisconnect(conn)
    }, add = TRUE)

    symbols <- fetch_symbols(conn)
    message(sprintf("Found %d symbols to process", length(symbols)))

    long_data <- split_batch(
      symbols = symbols,
      batch_size = 1,
      sleep_time = 2
    )

    batch_id <- as.integer(as.numeric(Sys.time()))
    timestamp <- Sys.time()

    if (!is.null(long_data) && nrow(long_data) > 0) {
      rows <- insert_new_data(conn, long_data)
      message(sprintf("Successfully inserted %d new rows", rows))
      # Log success
      summary_table <<- log_summary(
        summary_table = summary_table,
        user_login = user_login,
        batch_id = batch_id,
        symbol = paste(symbols, collapse = ","),
        status = "OK",
        n_rows = rows,
        message = "Data inserted successfully"
      )
    } else {
      message("No data to insert")
      # Log no data
      summary_table <<- log_summary(
        summary_table = summary_table,
        user_login = user_login,
        batch_id = batch_id,
        symbol = paste(symbols, collapse = ","),
        status = "OK",
        n_rows = 0,
        message = "No data to insert"
      )
    }

  }, error = function(e) {
    # Log error
    batch_id <- as.integer(as.numeric(Sys.time()))
    summary_table <<- log_summary(
      summary_table = summary_table,
      user_login = user_login,
      batch_id = batch_id,
      symbol = paste(symbols, collapse = ","),
      status = "ERROR",
      n_rows = 0,
      message = paste("Pipeline error:", e$message)
    )
    message("Error in pipeline: ", e$message)
    return(NULL)
  })
}

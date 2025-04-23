#' Insert new data into sp500.data table avoiding duplicates
#'
#' @description
#' This function inserts new records into the sp500.data table while checking for and
#' avoiding duplicate entries. It maps symbols to index_ts using the sp500.info table
#' and handles the insertion of new records in a safe and efficient manner.
#'
#' @param conn A DBI connection object to the PostgreSQL database
#' @param data A data frame containing the following columns:
#'   \itemize{
#'     \item symbol: Character. Stock symbol (e.g., "AAPL", "GOOGL")
#'     \item date: Date. The date of the observation
#'     \item variable: Character. The type of measurement (e.g., "open", "close", "high")
#'     \item value: Numeric. The actual value of the measurement
#'   }
#'
#' @return Integer. The number of rows successfully inserted into the database.
#'   Returns 0 if no new records were inserted, and NULL if an error occurred.
#'
#' @details
#' The function performs the following steps:
#' \enumerate{
#'   \item Validates the input parameters
#'   \item Maps stock symbols to index_ts using sp500.info table
#'   \item Identifies and removes any duplicate records
#'   \item Inserts only new, unique records into the database
#' }
#'
#' @note
#' The function will skip any symbols that are not found in the sp500.info table
#' and issue a warning with the list of missing symbols.
#'
#' @examples
#' \dontrun{
#' # Create sample data
#' sample_data <- data.frame(
#'   symbol = c("AAPL", "GOOGL"),
#'   date = as.Date(c("2023-01-01", "2023-01-01")),
#'   variable = c("close", "close"),
#'   value = c(150.23, 95.57)
#' )
#'
#' # Connect to database
#' conn <- connect_db()
#'
#' # Insert data
#' rows_inserted <- insert_new_data(conn, sample_data)
#'
#' # Close connection
#' DBI::dbDisconnect(conn)
#' }
#'
#' @importFrom DBI dbExecute dbGetQuery dbBegin dbCommit dbRollback
#' @importFrom dplyr left_join anti_join filter select rename
#' @importFrom glue glue_sql
#'
#' @export
insert_new_data <- function(conn, data) {
  tryCatch({
    # Input validation
    if (!inherits(conn, "DBIConnection")) {
      stop("conn must be a valid database connection")
    }
    if (!is.data.frame(data)) {
      stop("data must be a data frame")
    }
    required_cols <- c("symbol", "date", "variable", "value")
    if (!all(required_cols %in% names(data))) {
      stop("data must have columns: symbol, date, variable, value")
    }

    message("Fetching index_ts mapping...")
    # Get index_ts mapping from sp500.info
    index_mapping <- DBI::dbGetQuery(
      conn,
      glue::glue_sql("
        SELECT index_ts, symbol
        FROM sp500.info
        ",
                     .con = conn
      )
    )

    message("Joining data with index_ts mapping...")
    # Join to get index_ts
    data_with_index <- dplyr::left_join(
      data,
      index_mapping,
      by = "symbol"
    )

    # Check for missing symbols
    missing_symbols <- unique(data_with_index$symbol[is.na(data_with_index$index_ts)])
    if (length(missing_symbols) > 0) {
      warning(glue::glue("Symbols not found in sp500.info: {paste(missing_symbols, collapse = ', ')}"))
      data_with_index <- dplyr::filter(data_with_index, !is.na(index_ts))
    }

    message("Preparing data for insertion...")
    # Rename and select columns
    data_to_insert <- data_with_index |>
      dplyr::rename(metric = variable) |>
      dplyr::select(index_ts, date, metric, value)

    message("Checking for existing records...")
    # Get existing records
    existing <- DBI::dbGetQuery(
      conn,
      glue::glue_sql("
        SELECT index_ts, date, metric
        FROM student_gilmar.data_sp500
        ",
                     .con = conn
      )
    )

    message("Identifying new records...")
    # Find new records
    new_records <- dplyr::anti_join(
      data_to_insert,
      existing,
      by = c("index_ts", "date", "metric")
    )

    message("Starting insertion process...")
    if (nrow(new_records) > 0) {
      message(sprintf("Found %d new records to insert", nrow(new_records)))

      # Start transaction
      DBI::dbBegin(conn)

      tryCatch({
        # Prepare batch insert
        chunks <- split(new_records, ceiling(seq_len(nrow(new_records))/1000))
        total_inserted <- 0

        for (chunk in chunks) {
          values_sql <- paste(
            mapply(
              function(idx, dt, met, val) {
                glue::glue_sql(
                  "({index_ts}, {date}, {metric}, {value})",
                  index_ts = idx,
                  date = dt,
                  metric = met,
                  value = val,
                  .con = conn
                )
              },
              chunk$index_ts,
              chunk$date,
              chunk$metric,
              chunk$value,
              SIMPLIFY = TRUE
            ),
            collapse = ","
          )
          values_sql <- DBI::SQL(values_sql) # <- THIS LINE IS FUNDAMENTAL

          DBI::dbExecute(
            conn,
            glue::glue_sql(
              "INSERT INTO student_gilmar.data_sp500 (index_ts, date, metric, value)
       VALUES {values_sql}",
              .con = conn
            )
          )
          total_inserted <- total_inserted + nrow(chunk)
          message(sprintf("Inserted %d/%d records...", total_inserted, nrow(new_records)))
        }

        # Commit transaction
        DBI::dbCommit(conn)
        message("Successfully completed all insertions!")
        return(total_inserted)

      }, error = function(e) {
        # Rollback on error
        DBI::dbRollback(conn)
        message("Error during batch insert: ", e$message)

        message("Falling back to row-by-row insertion...")
        rows_inserted <- 0

        for (i in seq_len(nrow(new_records))) {
          tryCatch({
            DBI::dbExecute(
              conn,
              glue::glue_sql(
                "INSERT INTO student_gilmar.data_sp500 (index_ts, date, metric, value)
                 VALUES ({index_ts}, {date}, {metric}, {value})",
                index_ts = new_records$index_ts[i],
                date = new_records$date[i],
                metric = new_records$metric[i],
                value = new_records$value[i],
                .con = conn
              )
            )
            rows_inserted <- rows_inserted + 1
            if (i %% 100 == 0) {
              message(sprintf("Inserted %d/%d rows...", i, nrow(new_records)))
            }
          }, error = function(e) {
            message(sprintf("Error inserting row %d: %s", i, e$message))
          })
        }

        message(sprintf("Finished row-by-row insertion. Inserted %d/%d rows.",
                        rows_inserted, nrow(new_records)))
        return(rows_inserted)
      })
    } else {
      message("No new records to insert")
      return(0)
    }
  }, error = function(e) {
    message("Error occurred: ", e$message)
    return(NULL)
  })
}

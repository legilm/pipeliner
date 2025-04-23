#' Fetch S&P 500 Symbols
#'
#' Retrieves all stock symbols from the S&P 500 database, ordered by their index timestamp.
#'
#' @param conn A DBI connection object to the database
#' @return A character vector of stock symbols
#'
#' @export
#'
#' @examples
#' \dontrun{
#' conn <- connect_db()
#' symbols <- fetch_symbols(conn)
#' DBI::dbDisconnect(conn)
#' }
fetch_symbols <- function(conn) {
  if (!inherits(conn, "DBIConnection")) {
    stop("conn must be a valid database connection")
  }

  sql <- glue::glue_sql(
    "
    SELECT symbol
    FROM sp500.info
    ORDER BY index_ts ASC
    LIMIT 10
    ",
    .con = conn
  )

  res <- DBI::dbGetQuery(conn, sql)
  return(res$symbol)
}

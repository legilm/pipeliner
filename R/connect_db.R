#' Connect to PostgreSQL Database
#'
#' Establishes a connection to a PostgreSQL database using environment variables
#' for authentication and connection details.
#'
#' @return A DBI connection object to the PostgreSQL database
#' @export
#'
#' @examples
#' \dontrun{
#' # Set environment variables first
#' Sys.setenv(
#'   PG_DB = "your_database",
#'   PG_HOST = "your_host",
#'   PG_USER = "your_username",
#'   PG_PASSWORD = "your_password"
#' )
#'
#' # Connect to the database
#' con <- connect_db()
#'
#' # Don't forget to close the connection when done
#' DBI::dbDisconnect(con)
#' }
connect_db <- function() {
  con <- DBI::dbConnect(
    RPostgres::Postgres(),
    dbname = Sys.getenv("PG_DB"),
    host = Sys.getenv("PG_HOST"),
    user = Sys.getenv("PG_USER"),
    password = Sys.getenv("PG_PASSWORD"),
    port = 5432
  )
  return(con)
}

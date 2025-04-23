library(tidyquant)
#' Query Yahoo Finance for OHLCV stock data
#'
#' @param symbol Character. Stock ticker symbol (e.g., "AAPL").
#' @param from Date or character. Start date (e.g., "2020-01-01").
#' @param to Date or character. End date (e.g., "2020-12-31").
#' @return A data frame with columns: symbol, date, open, high, low, close, volume, adjusted.
#' @importFrom tidyquant tq_get
#' @export
yahoo_query_data <- function(symbol, from = "2024-04-01", to = Sys.Date() - 5) {
  # Input validation
  if (!is.character(symbol) || length(symbol) != 1) {
    stop("Symbol must be a single character string")
  }

  # Convert dates to Date objects
  tryCatch({
    from_date <- as.Date(from)
    to_date <- as.Date(to)
  }, error = function(e) {
    stop("Invalid date format. Use YYYY-MM-DD")
  })

  # Check date order
  if (from_date > to_date) {
    stop("Start date must be before or equal to end date")
  }

  # Query data with error handling
  result <- tryCatch({
    tq_get(
      x = symbol,
      from = from_date,
      to = to_date,
      get = "stock.prices"
    )
  }, error = function(e) {
    warning(paste("Error fetching data for", symbol, ":", e$message))
    return(NULL)
  })

  # Check if data was retrieved
  if (is.null(result) || nrow(result) == 0) {
    warning(paste("No data returned for symbol:", symbol))
    return(NULL)
  }

  return(result)
}

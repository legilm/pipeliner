#' Process Symbols in Batches
#'
#' @param symbols Character vector of stock symbols
#' @param batch_size Integer. Number of symbols to process in each batch
#' @param sleep_time Integer. Time to sleep between API calls in seconds
#' @return A data frame in long format with processed data
#' @importFrom dplyr bind_rows
#' @export
#'
split_batch <- function(symbols, batch_size = 1, sleep_time = 0.2) {
  stopifnot(
    is.character(symbols),
    is.numeric(batch_size),
    batch_size > 0,
    is.numeric(sleep_time),
    sleep_time >= 0
  )

  batches <- split(symbols, ceiling(seq_along(symbols) / batch_size))
  dados <- list()
  total_symbols <- length(symbols)
  processed <- 0

  for (lote in batches) {
    for (sym in lote) {
      processed <- processed + 1
      message(sprintf("Processing symbol: %s (%d/%d)", sym, processed, total_symbols))

      raw <- yahoo_query_data(sym)
      if (!is.null(raw)) {
        tryCatch({
          long <- format_data(raw)
          dados[[sym]] <- long
          message(sprintf("Successfully processed %s", sym))
        }, error = function(e) {
          warning(sprintf("Error processing %s: %s", sym, e$message))
        })
      }
      Sys.sleep(sleep_time)
    }
  }

  # Verifica se temos dados para combinar
  if (length(dados) == 0) {
    warning("No data was processed successfully")
    return(NULL)
  }

  # Combina todos os dados
  final_data <- dplyr::bind_rows(dados)
  message(sprintf("Finished processing. Total rows: %d", nrow(final_data)))

  return(final_data)
}

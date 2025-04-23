#' Format Financial Time Series Data
#'
#' @param df A data frame containing financial time series data
#' @param symbol Optional character string for the symbol if not in data
#' @return A data frame with columns: symbol, date, variable, value
#' @export
format_data <- function(df, symbol = NULL) {
  # Validações básicas
  if (!is.data.frame(df)) {
    stop("Input must be a data frame")
  }

  required_cols <- c("date", "open", "high", "low", "close", "volume", "adjusted")
  missing_cols <- setdiff(required_cols, names(df))
  if (length(missing_cols) > 0) {
    stop("Missing required columns: ", paste(missing_cols, collapse = ", "))
  }

  # Prepara dados
  if ("symbol" %in% names(df)) {
    clean_df <- df[, c("symbol", required_cols)]
  } else {
    clean_df <- df[, required_cols]
  }

  # Converte para formato long
  long_df <- tidyr::pivot_longer(
    data = clean_df,
    cols = setdiff(names(clean_df), c("date", "symbol")),
    names_to = "variable",
    values_to = "value"
  )

  # Adiciona símbolo se necessário
  if (!"symbol" %in% names(long_df) && !is.null(symbol)) {
    long_df$symbol <- symbol
  }

  # Organiza colunas
  if ("symbol" %in% names(long_df)) {
    long_df <- long_df[, c("symbol", "date", "variable", "value")]
  } else {
    long_df <- long_df[, c("date", "variable", "value")]
  }

  return(long_df)
}

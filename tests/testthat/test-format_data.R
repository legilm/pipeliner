library(testthat)

test_that("format_data() returns data frame in long format", {
  # Simulated example data
  df <- data.frame(
    date = as.Date(c("2023-01-01", "2023-01-02")),
    open = c(10, 20),
    high = c(15, 25),
    low = c(9, 19),
    close = c(14, 24),
    volume = c(1000, 2000),
    adjusted = c(14, 24)
  )

  long_df <- format_data(df)

  # Should be a data.frame
  expect_s3_class(long_df, "data.frame")

  # Should contain the correct columns
  expect_true(all(c("date", "variable", "value") %in% colnames(long_df)))

  # Should have 2 dates x 6 variables = 12 rows
  expect_equal(nrow(long_df), 12)

  # Should contain all original variables
  expect_setequal(unique(long_df$variable), c("open", "high", "low", "close", "volume", "adjusted"))
})

test_that("format_data() adds symbol column when provided", {
  df <- data.frame(
    date = as.Date("2023-01-01"),
    open = 10, high = 15, low = 9, close = 14, volume = 1000, adjusted = 14
  )
  long_df <- format_data(df, symbol = "AAPL")

  # Should have the symbol column
  expect_true("symbol" %in% colnames(long_df))

  # The value of the symbol column should be the one provided
  expect_true(all(long_df$symbol == "AAPL"))

  # Should have 6 rows (1 date x 6 variables)
  expect_equal(nrow(long_df), 6)
})

test_that("format_data() works with empty data frame", {
  df <- data.frame(
    date = as.Date(character()),
    open = numeric(),
    high = numeric(),
    low = numeric(),
    close = numeric(),
    volume = numeric(),
    adjusted = numeric()
  )
  long_df <- format_data(df)

  # Should return an empty data.frame
  expect_s3_class(long_df, "data.frame")
  expect_equal(nrow(long_df), 0)
})

# tests/testthat/test-yahoo_query_data.R

library(testthat)
library(tidyquant)

test_that("yahoo_query_data returns correct data structure", {
  # Skip if no internet connection
  skip_if_offline()

  result <- yahoo_query_data("AAPL", "2023-01-01", "2023-01-31")

  # Check if returns a data frame
  expect_s3_class(result, "data.frame")

  # Check column names
  expected_cols <- c("symbol", "date", "open", "high", "low", "close", "volume", "adjusted")
  expect_named(result, expected_cols)

  # Check column types
  expect_type(result$symbol, "character")
  expect_s3_class(result$date, "Date")
  expect_type(result$open, "double")
  expect_type(result$high, "double")
  expect_type(result$low, "double")
  expect_type(result$close, "double")
  expect_type(result$volume, "double")
  expect_type(result$adjusted, "double")

  # Check data is not empty
  expect_true(nrow(result) > 0)
})

test_that("yahoo_query_data handles date inputs correctly", {
  skip_if_offline()

  # Test with character dates
  result1 <- yahoo_query_data("AAPL", "2023-01-01", "2023-01-31")
  expect_true(min(result1$date) >= as.Date("2023-01-01"))
  expect_true(max(result1$date) <= as.Date("2023-01-31"))

  # Test with Date objects
  result2 <- yahoo_query_data("AAPL",
                              as.Date("2023-01-01"),
                              as.Date("2023-01-31"))
  expect_equal(result1, result2, tolerance = 1e-3)
})

test_that("yahoo_query_data handles invalid inputs appropriately", {
  skip_if_offline()

  # Invalid symbol
  expect_error(yahoo_query_data("INVALID_SYMBOL_XXX"))

  # Invalid date format
  expect_error(yahoo_query_data("AAPL", "invalid_date"))

  # End date before start date
  expect_error(yahoo_query_data("AAPL", "2023-01-31", "2023-01-01"))
})

test_that("yahoo_query_data works with different markets", {
  skip_if_offline()

  # Test US stock
  us_stock <- yahoo_query_data("AAPL", "2023-01-01", "2023-01-31")
  expect_true(nrow(us_stock) > 0)

})

test_that("yahoo_query_data handles default dates correctly", {
  skip_if_offline()

  # Test with only symbol
  result <- yahoo_query_data("AAPL")

  expect_true(nrow(result) > 0)
  expect_true(min(result$date) >= as.Date("2000-01-01"))
  expect_true(max(result$date) <= Sys.Date())
})

test_that("yahoo_query_data returns ordered dates", {
  skip_if_offline()

  result <- yahoo_query_data("AAPL", "2023-01-01", "2023-01-31")

  # Check if dates are in ascending order
  expect_true(all(diff(result$date) >= 0))
})

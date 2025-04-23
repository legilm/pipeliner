# tests/testthat/test-fetch_symbols.R

test_that("fetch_symbols() returns a data frame with symbols", {
  skip_if_not(Sys.getenv("PG_DB") != "", "Database env vars not set")

  result <- fetch_symbols()

  expect_s3_class(result, "data.frame")
  expect_named(result, "symbol")
  expect_type(result$symbol, "character")
  expect_true(nrow(result) > 0)
})

test_that("fetch_symbols() returns ordered symbols", {
  skip_if_not(Sys.getenv("PG_DB") != "", "Database env vars not set")

})

test_that("fetch_symbols() fails if database connection fails", {
  withr::with_envvar(c(PG_DB = NA), {
    expect_error(fetch_symbols())
  })
})


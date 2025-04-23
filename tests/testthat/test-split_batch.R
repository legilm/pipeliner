# tests/testthat/test-split_batch.R

test_that("split_batch splits vector correctly", {
  x <- 1:10
  out <- split_batch(x, batch_size = 3)
  expect_length(out, 4)
  expect_equal(out[[1]], 1:3)
  expect_equal(out[[4]], 10)
})

test_that("split_batch splits data frame correctly", {
  df <- data.frame(a = 1:7, b = letters[1:7])
  out <- split_batch(df, batch_size = 2)
  expect_length(out, 4)
  expect_equal(nrow(out[[1]]), 2)
  expect_equal(nrow(out[[4]]), 1)
  expect_equal(out[[4]]$a, 7)
})

test_that("split_batch errors on invalid batch_size", {
  expect_error(split_batch(1:5, batch_size = 0))
  expect_error(split_batch(1:5, batch_size = -1))
  expect_error(split_batch(1:5, batch_size = "a"))
})

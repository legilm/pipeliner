# test-connect_db.R

test_that("connect_db() returns a PqConnection", {
  skip_if_not(Sys.getenv("PG_DB") != "", "Database env vars not set")

  con <- connect_db()
  expect_s4_class(con, "PqConnection")

  DBI::dbDisconnect(con)
})

test_that("connect_db() throws error if env vars are missing", {
  withr::with_envvar(c(PG_DB = NA, PG_HOST = NA, PG_USER = NA, PG_PASSWORD = NA), {
    expect_error(connect_db())
  })
})

test_that("connect_db() throws error if only some env vars are missing", {
  withr::with_envvar(c(PG_DB = NA), {
    expect_error(connect_db())
  })

  withr::with_envvar(c(PG_HOST = NA), {
    expect_error(connect_db())
  })
})

test_that("connect_db() throws error with invalid credentials", {
  skip_if_not(Sys.getenv("PG_DB") != "", "Database env vars not set")

  withr::with_envvar(c(PG_PASSWORD = "wrong_password"), {
    expect_error(connect_db())
  })
})

test_that("connect_db() throws error with invalid host", {
  skip_if_not(Sys.getenv("PG_DB") != "", "Database env vars not set")

  withr::with_envvar(c(PG_HOST = "invalid_host"), {
    expect_error(connect_db())
  })
})

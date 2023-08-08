### tests for general functions ###

test_that("date functions work", {
  mth <- fdaysInMn(12, 2020)
  expect_vector(mth)
  expect_length(mth, 31)
  
  yr <- fdtRanges(2020)
  expect_vector(yr)
  expect_length(yr, 12)
})

test_that('utility functions work', {
  expect_no_error(assert_range(1, 0:2))
  expect_error(assert_range(1, 0))
  
  expect_true(is_truthy(1))
  expect_true(is_truthy(0))
  expect_true(is_truthy(data.frame(a=1)))
  expect_false(is_truthy(data.frame()))
})

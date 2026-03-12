test_that("prepare_source_table returns correct structure", {
  dittodb::with_mock_db({
    con_test <- make_test_connection()
    result <- prepare_source_table(con_test)
    expect_s3_class(result, "data.frame")
    expect_equal(nrow(result), 1)
    expect_equal(ncol(result), 4)
    expect_true(all(c("id", "name", "name_long", "url") %in% names(result)))
    expect_equal(result$name, "SPGlobal")
    expect_equal(result$name_long, "S&P Global Market Intelligence")
    expect_type(result$id, "double")
    DBI::dbDisconnect(con_test)
  })
})


test_that("prepare_table_table returns correct structure for eurozone", {
  dittodb::with_mock_db({
    con_test <- make_test_connection()
    result <- prepare_table_table("eurozone", 9, con_test)
    expect_s3_class(result, "data.frame")
    expect_equal(nrow(result), 1)
    expect_equal(ncol(result), 6)
    expect_true(all(c("code", "name", "source_id", "url",
                      "notes", "keep_vintage") %in% names(result)))
    expect_equal(result$code, "SPGlobal-HCOB-EZ")
    expect_equal(result$name, "HCOB Eurozone PMI")
    expect_false(result$keep_vintage)
    DBI::dbDisconnect(con_test)
  })
})

test_that("prepare_table_table errors on unknown config", {
  dittodb::with_mock_db({
    con_test <- make_test_connection()
    expect_error(prepare_table_table("narnia", con_test), "Unknown config")
    DBI::dbDisconnect(con_test)
  })
})

test_that("prepare_table_dimensions_table returns correct structure", {
  dittodb::with_mock_db({
    con_test <- make_test_connection()
    result <- prepare_table_dimensions_table("eurozone", con_test)
    expect_s3_class(result, "data.frame")
    expect_equal(nrow(result), 1)
    expect_true(all(c("table_id", "dimension", "is_time") %in% names(result)))
    expect_equal(result$dimension, "indicator")
    expect_false(result$is_time)
    DBI::dbDisconnect(con_test)
  })
})

test_that("prepare_dimension_levels_table returns correct structure", {
  dittodb::with_mock_db({
    con_test <- make_test_connection()
    result <- prepare_dimension_levels_table("eurozone", con_test)
    expect_s3_class(result, "data.frame")
    expect_equal(nrow(result), 4)
    expect_true(all(c("tab_dim_id", "level_value", "level_text") %in% names(result)))
    expect_true(all(c("composite", "services", "manufacturing",
                      "manufacturing_output") %in% result$level_value))
    # All rows should have the same tab_dim_id
    expect_equal(length(unique(result$tab_dim_id)), 1)
    DBI::dbDisconnect(con_test)
  })
})

test_that("prepare_series_table returns correct structure", {
  dittodb::with_mock_db({
    con_test <- make_test_connection()
    result <- prepare_series_table("eurozone", con_test)
    expect_s3_class(result, "data.frame")
    expect_equal(nrow(result), 4)
    expect_true(all(c("table_id", "name_long", "unit_id",
                      "code", "interval_id") %in% names(result)))
    # All series should be monthly
    expect_true(all(result$interval_id == "M"))
    # All series should have the same table_id
    expect_equal(length(unique(result$table_id)), 1)
    # Check series codes follow convention
    expect_true(all(grepl("^SPGlobal-HCOB-EZ--.*--M$", result$code)))
    expected_codes <- c(
      "SPGlobal-HCOB-EZ--composite--M",
      "SPGlobal-HCOB-EZ--services--M",
      "SPGlobal-HCOB-EZ--manufacturing--M",
      "SPGlobal-HCOB-EZ--manufacturing_output--M"
    )
    expect_true(all(expected_codes %in% result$code))
    DBI::dbDisconnect(con_test)
  })
})

test_that("prepare_series_levels_table returns correct structure", {
  dittodb::with_mock_db({
    con_test <- make_test_connection()
    result <- prepare_series_levels_table("eurozone", con_test)
    expect_s3_class(result, "data.frame")
    expect_equal(nrow(result), 4)
    expect_true(all(c("series_id", "tab_dim_id",
                      "level_value") %in% names(result)))
    # Each series should map to one level
    expect_equal(length(unique(result$series_id)), 4)
    expect_true(all(c("composite", "services", "manufacturing",
                      "manufacturing_output") %in% result$level_value))
    DBI::dbDisconnect(con_test)
  })
})

test_that("pmi_config returns all expected regions", {
  config <- pmi_config()
  expect_type(config, "list")
  expect_true(all(c("eurozone", "germany", "france", "italy", "spain") %in%
                    names(config)))
  # Each region should have 4 series
  purrr::walk(config, \(region) {
    expect_equal(length(region$series), 4)
    indicators <- purrr::map_chr(region$series, "indicator")
    expect_true(all(c("composite", "services", "manufacturing",
                      "manufacturing_output") %in% indicators))
  })
})

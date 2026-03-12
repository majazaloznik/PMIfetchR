source("tests/testthat/helper-connection.R")

# Run this script section by section to record dittodb fixtures.
# Each section records the DB queries for one prepare function,
# then inserts the result so the next section can look it up.
#
# Prerequisites:
# - Connection to production_backup database
# - UMARimportR and UMARaccessR installed
# - PMIfetchR loaded


# =============================================================================
# 1. Source
# =============================================================================
dittodb::start_db_capturing()
con_test <- make_test_connection()
source_df <- prepare_source_table(con_test)
dittodb::stop_db_capturing()

# Now insert so subsequent queries can find it
UMARimportR::insert_new_source(con_test, source_df)

# =============================================================================
# 3. Table
# =============================================================================
dittodb::start_db_capturing()
con_test <- make_test_connection()
table_df <- prepare_table_table("eurozone", 9, con_test)
dittodb::stop_db_capturing()

# Insert
UMARimportR::insert_new_table_table(con_test, table_df)


# =============================================================================
# 4. Table dimensions
# =============================================================================
dittodb::start_db_capturing()
con_test <- make_test_connection()
prepare_table_dimensions_table("eurozone", con_test)
dittodb::stop_db_capturing()
DBI::dbDisconnect(con_test)

# Insert
con <- make_test_connection()
dim_df <- prepare_table_dimensions_table("eurozone", con)
UMARimportR::insert_new_table_dimensions(con, dim_df)
DBI::dbDisconnect(con)

# =============================================================================
# 5. Dimension levels
# =============================================================================
dittodb::start_db_capturing()
con_test <- make_test_connection()
prepare_dimension_levels_table("eurozone", con_test)
dittodb::stop_db_capturing()
DBI::dbDisconnect(con_test)

# Insert
con <- make_test_connection()
levels_df <- prepare_dimension_levels_table("eurozone", con)
purrr::walk(seq_len(nrow(levels_df)), \(i) {
  UMARimportR::insert_new_dimension_levels(con, levels_df[i, ])
})
DBI::dbDisconnect(con)

# =============================================================================
# 6. Series
# =============================================================================
dittodb::start_db_capturing()
con_test <- make_test_connection()
prepare_series_table("eurozone", con_test)
dittodb::stop_db_capturing()
DBI::dbDisconnect(con_test)

# Insert
con <- make_test_connection()
series_df <- prepare_series_table("eurozone", con)
purrr::walk(seq_len(nrow(series_df)), \(i) {
  UMARimportR::insert_new_series(con, series_df[i, ])
})
DBI::dbDisconnect(con)

# =============================================================================
# 7. Series levels
# =============================================================================
dittodb::start_db_capturing()
con_test <- make_test_connection()
prepare_series_levels_table("eurozone", con_test)
dittodb::stop_db_capturing()
DBI::dbDisconnect(con_test)

# Insert
con <- make_test_connection()
series_levels_df <- prepare_series_levels_table("eurozone", con)
purrr::walk(seq_len(nrow(series_levels_df)), \(i) {
  UMARimportR::insert_new_series_levels(con, series_levels_df[i, ])
})
DBI::dbDisconnect(con)

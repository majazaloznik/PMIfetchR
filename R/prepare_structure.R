#' PMIfetchR - Prepare structure functions
#'
#' Prepare data frames for insertion into the database via
#' UMARimportR::insert_new_* functions. Follows the same pattern
#' as EUROSTATfetchR/SURSfetchR etc..
#'
#' Each PMI region gets:
#' - 1 source (shared): "SPGlobal"
#' - 1 table per region: e.g. "SPGlobal-HCOB-EZ"
#' - 1 dimension: "indicator" (not time)
#' - 4 dimension levels: composite, services, manufacturing, manufacturing_output
#' - 4 series (one per indicator), all monthly
#' - No categories (flat structure, no hierarchy needed)

# =============================================================================
# Source
# =============================================================================

#' Prepare row for the `source` table
#'
#' @param con database connection
#' @param schema database schema, default "platform"
#' @return data frame ready for UMARimportR::insert_new_source(), or message
#'   if source already exists
#' @export
prepare_source_table <- function(con, schema = "platform") {
  source_id <- UMARaccessR::sql_get_source_code_from_source_name(
    con, "SPGlobal", schema
  )
  if (is.null(source_id)) {
    DBI::dbExecute(con, paste0("set search_path to ", schema))
    id <- dplyr::tbl(con, "source") |>
      dplyr::summarise(max = max(id, na.rm = TRUE)) |>
      dplyr::pull() + 1
    data.frame(
      id = id,
      name = "SPGlobal",
      name_long = "S&P Global Market Intelligence",
      url = "https://www.pmi.spglobal.com"
    )
  } else {
    message("SPGlobal already listed in the source table.")
    NULL
  }
}



# =============================================================================
# Table
# =============================================================================

#' Prepare row for the `table` table
#'
#' @param config_name character, one of names(pmi_config()): "eurozone",
#'   "germany", "france", "italy", "spain"
#' @param source_id probably 9
#' @param con database connection
#' @param schema database schema, default "platform"
#' @return data frame ready for UMARimportR::insert_new_table_table()
#' @export
prepare_table_table <- function(config_name, source_id, con, schema = "platform") {
  config <- pmi_config()[[config_name]]
  if (is.null(config)) stop("Unknown config: ", config_name)
  data.frame(
    code = config$table_code,
    name = config$table_name,
    source_id = source_id,
    url = "https://www.pmi.spglobal.com",
    notes = as.character(jsonlite::toJSON(list(), auto_unbox = TRUE)),
    keep_vintage = FALSE
  )
}

# =============================================================================
# Table dimensions
# =============================================================================

#' Prepare row for the `table_dimensions` table
#'
#' PMI tables have a single non-time dimension: "indicator"
#'
#' @param config_name character
#' @param con database connection
#' @param schema database schema, default "platform"
#' @return data frame ready for UMARimportR::insert_new_table_dimensions()
#' @export
prepare_table_dimensions_table <- function(config_name, con, schema = "platform") {
  config <- pmi_config()[[config_name]]
  tbl_id <- UMARaccessR::sql_get_table_id_from_table_code(
    con, config$table_code, schema
  )

  data.frame(
    table_id = tbl_id,
    dimension = "indicator",
    is_time = FALSE
  )
}

# =============================================================================
# Dimension levels
# =============================================================================

#' Prepare rows for the `dimension_levels` table
#'
#' One level per indicator: composite, services, manufacturing, manufacturing_output
#'
#' @param config_name character
#' @param con database connection
#' @param schema database schema, default "platform"
#' @return data frame ready for UMARimportR::insert_new_dimension_levels()
#'   (one row per level)
#' @export
prepare_dimension_levels_table <- function(config_name, con, schema = "platform") {
  config <- pmi_config()[[config_name]]
  tbl_id <- UMARaccessR::sql_get_table_id_from_table_code(
    con, config$table_code, schema
  )

  tab_dim_id <- UMARaccessR::sql_get_dimension_id_from_table_id_and_dimension(
    tbl_id, "indicator", con, schema
  )

  level_texts <- c(
    composite = "Composite PMI Output Index",
    services = "Services PMI Business Activity Index",
    manufacturing = "Manufacturing PMI",
    manufacturing_output = "Manufacturing PMI Output Index"
  )

  data.frame(
    tab_dim_id = tab_dim_id,
    level_value = names(level_texts),
    level_text = unname(level_texts)
  )
}

# =============================================================================
# Series
# =============================================================================

#' Prepare rows for the `series` table
#'
#' Creates 4 series per region table (one per indicator).
#'
#' @param config_name character
#' @param con database connection
#' @param schema database schema, default "platform"
#' @return data frame ready for UMARimportR::insert_new_series()
#'   (one row per series)
#' @export
prepare_series_table <- function(config_name, con, schema = "platform") {
  config <- pmi_config()[[config_name]]
  tbl_id <- UMARaccessR::sql_get_table_id_from_table_code(
    con, config$table_code, schema
  )

  unit_id <- DBI::dbGetQuery(
    con,
    paste0("SELECT id FROM ", schema, ".unit WHERE name = 'indeks'")
  )$id[1]

  data.frame(
    table_id = tbl_id,
    name_long = purrr::map_chr(config$series, "name_long"),
    unit_id = unit_id,
    code = purrr::map_chr(config$series, \(s) {
      paste0(config$table_code, "--", s$code_suffix, "--M")
    }),
    interval_id = "M"
  )
}

# =============================================================================
# Series levels
# =============================================================================

#' Prepare rows for the `series_levels` table
#'
#' Links each series to its indicator dimension level.
#'
#' @param config_name character
#' @param con database connection
#' @param schema database schema, default "platform"
#' @return data frame ready for UMARimportR::insert_new_series_levels()
#'   (one row per series)
#' @export
prepare_series_levels_table <- function(config_name, con, schema = "platform") {
  config <- pmi_config()[[config_name]]
  tbl_id <- UMARaccessR::sql_get_table_id_from_table_code(
    con, config$table_code, schema
  )

  tab_dim_id <- UMARaccessR::sql_get_dimension_id_from_table_id_and_dimension(
    tbl_id, "indicator", con, schema
  )

  # Get series IDs from DB
  series_df <- UMARaccessR::sql_get_series_from_table_id(tbl_id, con, schema)

  purrr::map_dfr(config$series, \(s) {
    series_code <- paste0(config$table_code, "--", s$code_suffix, "--M")
    series_id <- series_df$id[series_df$code == series_code]

    if (length(series_id) == 0) {
      warning("Series not found: ", series_code)
      return(NULL)
    }

    data.frame(
      series_id = series_id,
      tab_dim_id = tab_dim_id,
      level_value = s$code_suffix
    )
  })
}

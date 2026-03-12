
# =============================================================================
# Configuration
# =============================================================================

#' Define PMI table/series configuration
#'
#' Each region gets one table with 4 series:
#' - composite: Composite PMI Output Index
#' - services: Services PMI Business Activity Index
#' - manufacturing: Manufacturing PMI (headline)
#' - manufacturing_output: Manufacturing PMI Output Index
#'
#' Release structure:
#' - Eurozone/Germany: flash (~20th) + final manufacturing (~1st) +
#'   final composite/services (~3rd-4th)
#' - Italy/France/Spain: no flash, manufacturing (~1st) + services (~3rd-4th)
#'
#' @return List of table configurations
#' @export
pmi_config <- function() {
  make_region <- function(code, name, region) {
    list(
      table_code = paste0("SPGlobal-HCOB-", code),
      table_name = paste("HCOB", name, "PMI"),
      region = region,
      series = list(
        list(
          indicator = "composite",
          code_suffix = "composite",
          name_long = paste("HCOB", name, "Composite PMI Output Index")
        ),
        list(
          indicator = "services",
          code_suffix = "services",
          name_long = paste("HCOB", name, "Services PMI Business Activity Index")
        ),
        list(
          indicator = "manufacturing",
          code_suffix = "manufacturing",
          name_long = paste("HCOB", name, "Manufacturing PMI")
        ),
        list(
          indicator = "manufacturing_output",
          code_suffix = "manufacturing_output",
          name_long = paste("HCOB", name, "Manufacturing PMI Output Index")
        )
      )
    )
  }

  list(
    eurozone = make_region("EZ", "Eurozone", "Eurozone"),
    germany  = make_region("DE", "Germany", "Germany"),
    france   = make_region("FR", "France", "France"),
    italy    = make_region("IT", "Italy", "Italy"),
    spain    = make_region("ES", "Spain", "Spain")
  )
}

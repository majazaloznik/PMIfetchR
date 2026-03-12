#' PMIfetchR - Extract functions
#'
#' Scrapes S&P Global PMI press releases from pmi.spglobal.com
#'
#' Release structure per month:
#' - ~20th: Flash release (combined: composite, services, manufacturing)
#'   Title: "HCOB Flash Eurozone PMI"
#' - ~1st business day of next month: Final Manufacturing
#'   Title: "HCOB Eurozone Manufacturing PMI"
#' - ~3rd-4th business day: Final Composite + Services
#'   Title: "HCOB Eurozone Composite PMI"
#'
#' For Germany: same pattern with "Germany" instead of "Eurozone"
#'
#' Discovery of release URLs:
#' The listing and calendar pages at pmi.spglobal.com are behind JS/Cloudflare
#' protection and cannot be scraped reliably. Instead, release URLs (GUIDs) are
#' maintained in a package-internal CSV config file. The individual press release
#' pages (PDF or HTML) are accessible without JS and parse reliably.
#'
#' To add new releases, update the CSV with `add_release_url()` or edit
#' inst/extdata/release_urls.csv directly.

# =============================================================================
# Shared request headers
# =============================================================================

#' Add browser-like headers to an httr2 request
#'
#' pmi.spglobal.com rejects requests with bot-like User-Agent strings (403).
#'
#' @param req An httr2 request object
#' @return Modified request object
#' @keywords internal
pmi_req_headers <- function(req) {
  httr2::req_headers(
    req,
    `User-Agent` = paste0(
      "Mozilla/5.0 (Windows NT 10.0; Win64; x64) ",
      "AppleWebKit/537.36 (KHTML, like Gecko) ",
      "Chrome/131.0.0.0 Safari/537.36"
    ),
    `Accept` = "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    `Accept-Language` = "en-US,en;q=0.9",
    `Accept-Encoding` = "gzip, deflate, br"
  )
}

# =============================================================================
# Release listing
# =============================================================================

#' Fetch the press release listing page and extract release metadata
#'
#' Parses the structured HTML from pmi.spglobal.com using CSS classes:
#' span.releaseDate, span.releaseTitle, and the PressRelease link.
#' Uses cookie persistence to handle any Cloudflare challenges.
#'
#' @param language Character. Language code, default "en".
#' @return Data frame with columns: date_utc, title, url
#' @export
get_release_list <- function(language = "en") {
  url <- paste0(
    "https://www.pmi.spglobal.com/Public/Release/PressReleases?language=",
    language
  )

  cookie_jar <- tempfile()
  on.exit(unlink(cookie_jar), add = TRUE)

  response <- httr2::request(url) |>
    pmi_req_headers() |>
    httr2::req_cookie_preserve(cookie_jar) |>
    httr2::req_perform()

  html <- rvest::read_html(httr2::resp_body_string(response))

  # Each release is in a div.listItem containing:
  #   span.releaseDate  - "March 04 2026 09:00 UTC"
  #   span.releaseTitle - "HCOB Eurozone Composite PMI"
  #   a[href*=PressRelease] - the link
  items <- html |> rvest::html_elements("div.listItem")

  if (length(items) == 0) {
    # Site returned JS shell without content - return NULL to signal failure
    return(NULL)
  }

  dates <- items |>
    rvest::html_element("span.releaseDate") |>
    rvest::html_text(trim = TRUE) |>
    gsub("\u00a0", " ", x = _)  # replace &nbsp; with space

  titles <- items |>
    rvest::html_element("span.releaseTitle") |>
    rvest::html_text(trim = TRUE)

  urls <- items |>
    rvest::html_element("a[href*='PressRelease']") |>
    rvest::html_attr("href")

  data.frame(
    date_utc = dates,
    title = titles,
    url = urls,
    stringsAsFactors = FALSE
  )
}

# =============================================================================
# Release URL registry (fallback when listing page is JS-blocked)
# =============================================================================

#' Path to the release URL registry CSV
#' @keywords internal
release_registry_path <- function() {
  system.file("extdata", "release_urls.csv", package = "PMIfetchR")
}

#' Get all registered release URLs
#'
#' @return Data frame with columns: date_utc, title, url
#' @export
get_release_registry <- function() {
  path <- release_registry_path()
  if (!file.exists(path) || file.size(path) < 10) {
    return(data.frame(date_utc = character(), title = character(),
                      url = character(), stringsAsFactors = FALSE))
  }
  utils::read.csv(path, stringsAsFactors = FALSE)
}

#' Add a release URL to the registry
#'
#' @param date_utc Character. e.g. "March 02 2026 09:00 UTC"
#' @param title Character. e.g. "HCOB Eurozone Manufacturing PMI"
#' @param url Character. Full URL to the press release.
#' @export
add_release_url <- function(date_utc, title, url) {
  path <- release_registry_path()
  if (!file.exists(path)) init_release_registry()
  registry <- get_release_registry()

  if (url %in% registry$url) {
    message("URL already in registry, skipping.")
    return(invisible(registry))
  }

  new_row <- data.frame(date_utc = date_utc, title = title,
                        url = url, stringsAsFactors = FALSE)
  registry <- rbind(registry, new_row)
  utils::write.csv(registry, path, row.names = FALSE)
  message("Added: ", title, " (", date_utc, ")")
  invisible(registry)
}

#' Initialise an empty release registry
#' @export
init_release_registry <- function() {
  dir_path <- file.path(system.file(package = "PMIfetchR"), "extdata")
  if (!dir.exists(dir_path)) dir.create(dir_path, recursive = TRUE)
  path <- file.path(dir_path, "release_urls.csv")
  if (!file.exists(path)) {
    empty <- data.frame(date_utc = character(), title = character(),
                        url = character(), stringsAsFactors = FALSE)
    utils::write.csv(empty, path, row.names = FALSE)
    message("Created empty release registry at: ", path)
  }
  invisible(path)
}

# =============================================================================
# Filtering
# =============================================================================

#' Filter release list for relevant releases
#'
#' @param releases Data frame from get_release_list()
#' @param region Character. "Eurozone" or "Germany"
#' @param type Character. "flash", "final_composite", "final_manufacturing", or "all"
#' @return Filtered data frame
#' @export
filter_releases <- function(releases, region = "Eurozone", type = "all") {
  patterns <- list(
    flash = paste0("^HCOB Flash ", region, " PMI$"),
    final_composite = paste0("^HCOB ", region, " Composite PMI$"),
    final_manufacturing = paste0("^HCOB ", region, " Manufacturing PMI$")
  )

  if (type == "all") {
    pattern <- paste(unlist(patterns), collapse = "|")
  } else {
    pattern <- patterns[[type]]
    if (is.null(pattern)) stop("Invalid type: ", type)
  }

  releases[grepl(pattern, releases$title), ]
}

# =============================================================================
# Press release parsing
# =============================================================================

#' Fetch and parse a single PMI press release
#'
#' @param release_url Character. Full URL to the press release.
#' @param publication_date Date or NULL. If provided, used as the publication
#'   date instead of parsing from the release text. Recommended because PDF
#'   releases often lack an embargo line.
#' @return List with components: publication_date, reference_period,
#'   is_flash, indicators (data frame of indicator/value pairs)
#' @export
extract_pmi_from_release <- function(release_url, publication_date = NULL) {
  response <- httr2::request(release_url) |>
    pmi_req_headers() |>
    httr2::req_perform()

  content_type <- httr2::resp_content_type(response)

  if (grepl("pdf", content_type, ignore.case = TRUE)) {
    tmp <- tempfile(fileext = ".pdf")
    writeBin(httr2::resp_body_raw(response), tmp)
    text <- paste(pdftools::pdf_text(tmp), collapse = "\n")
    unlink(tmp)
  } else {
    html <- rvest::read_html(httr2::resp_body_string(response))
    text <- rvest::html_text(html)
  }

  text <- gsub("\r", "", text)

  parse_release_text(text, release_url, publication_date)
}

#' Parse the text content of a press release
#'
#' @param text Character. Full text of the press release.
#' @param url Character. Source URL for reference.
#' @param publication_date Date or NULL. If provided, overrides parsing from text.
#' @return List with publication_date, reference_period, is_flash, indicators
#' @keywords internal
parse_release_text <- function(text, url = NA_character_,
                               publication_date = NULL) {
  # --- Determine if flash or final ---
  header <- substr(text, 1, 500)
  is_flash <- grepl("HCOB\\s+Flash", header)

  # --- Extract publication date ---
  if (!is.null(publication_date)) {
    pub_date <- publication_date
  } else {
    embargo_match <- stringr::str_match(
      text,
      "(?:Embargoed|Embargo).*?(\\d{1,2})\\s+(January|February|March|April|May|June|July|August|September|October|November|December)\\s+(\\d{4})"
    )
    if (!is.na(embargo_match[1, 1])) {
      pub_date <- as.Date(
        paste(embargo_match[1, 2], embargo_match[1, 3], embargo_match[1, 4]),
        format = "%d %B %Y"
      )
    } else {
      warning("Could not extract publication date from: ", url)
      pub_date <- NA
    }
  }

  # --- Extract reference period ---
  # "Data were collected DD-DD Month [YYYY]"
  # In PDFs this can span lines and year is often missing.
  text_normalised <- gsub("\\s+", " ", text)

  data_collected <- stringr::str_match(
    text_normalised,
    "Data were collected\\s+\\d{1,2}\\s*[-\u2013]\\s*\\d{1,2}\\s+(January|February|March|April|May|June|July|August|September|October|November|December)(?:\\s+(\\d{4}))?"
  )

  if (!is.na(data_collected[1, 2])) {
    ref_month <- data_collected[1, 2]
    ref_year <- if (!is.na(data_collected[1, 3])) {
      data_collected[1, 3]
    } else if (!is.na(pub_date)) {
      pub_year <- as.integer(format(pub_date, "%Y"))
      ref_month_num <- match(ref_month, month.name)
      pub_month_num <- as.integer(format(pub_date, "%m"))
      if (ref_month_num > pub_month_num) {
        as.character(pub_year - 1L)
      } else {
        as.character(pub_year)
      }
    } else {
      NA_character_
    }
    if (!is.na(ref_year)) {
      reference_period <- paste0(
        ref_year, "M",
        sprintf("%02d", match(ref_month, month.name))
      )
    } else {
      reference_period <- NA_character_
    }
  } else {
    warning("Could not extract reference period from: ", url)
    reference_period <- NA_character_
  }

  # --- Extract indicator values from Key findings ---
  indicator_pattern <- paste0(
    "((?:Composite\\s+PMI\\s+Output\\s+Index|",
    "Services\\s+PMI\\s+Business\\s+Activity\\s+Index|",
    "Manufacturing\\s+PMI\\s+Output\\s+Index|",
    "Manufacturing\\s+PMI)",
    "\\s*\\(?\\d*\\)?)",
    "\\s+at\\s+(\\d+\\.\\d)",
    "(?:\\s*\\(\\s*\\w+\\.?:\\s*(\\d+\\.\\d)\\s*\\))?"
  )

  matches <- stringr::str_match_all(text_normalised, indicator_pattern)[[1]]

  if (nrow(matches) == 0) {
    warning("No indicator values found in: ", url)
    return(list(
      publication_date = pub_date,
      reference_period = reference_period,
      is_flash = is_flash,
      indicators = data.frame(
        indicator = character(),
        value = numeric(),
        previous = numeric(),
        stringsAsFactors = FALSE
      ),
      url = url
    ))
  }

  indicators <- data.frame(
    indicator = dplyr::case_when(
      grepl("Composite\\s+PMI\\s+Output", matches[, 2]) ~ "composite",
      grepl("Services\\s+PMI", matches[, 2])             ~ "services",
      grepl("Manufacturing\\s+PMI\\s+Output", matches[, 2]) ~ "manufacturing_output",
      grepl("Manufacturing\\s+PMI", matches[, 2])         ~ "manufacturing",
      TRUE ~ NA_character_
    ),
    value = as.numeric(matches[, 3]),
    previous = as.numeric(matches[, 4]),
    stringsAsFactors = FALSE
  )

  indicators <- indicators[!duplicated(indicators$indicator), ]

  list(
    publication_date = pub_date,
    reference_period = reference_period,
    is_flash = is_flash,
    indicators = indicators,
    url = url
  )
}

# =============================================================================
# High-level extract function
# =============================================================================

#' Extract PMI data from latest releases for a region
#'
#' Tries to scrape the release listing page first. If the site returns
#' a JS-only shell (which happens intermittently), falls back to the
#' manually maintained release registry CSV.
#'
#' @param region Character. "Eurozone", "Germany", "France", "Italy", or "Spain"
#' @return List of parsed releases
#' @export
get_latest_pmi <- function(region = "Eurozone") {
  # Try scraping the listing page
  releases <- tryCatch(get_release_list(), error = function(e) NULL)

  if (is.null(releases)) {
    message("Listing page unavailable (JS-rendered). Using release registry.")
    releases <- get_release_registry()
  }

  if (nrow(releases) == 0) {
    warning("No releases found (listing failed and registry is empty).")
    return(list())
  }

  relevant <- filter_releases(releases, region = region, type = "all")

  if (nrow(relevant) == 0) {
    warning("No relevant releases found for region: ", region)
    return(list())
  }

  # Parse publication date from listing
  date_strings <- stringr::str_extract(
    relevant$date_utc,
    "^\\w+\\s+\\d{1,2}\\s+\\d{4}"
  )
  pub_dates <- as.Date(date_strings, format = "%B %d %Y")

  purrr::map2(relevant$url, pub_dates, \(url, pub_date) {
    tryCatch(
      extract_pmi_from_release(url, publication_date = pub_date),
      error = function(e) {
        warning("Failed to parse release: ", url, "\n", conditionMessage(e))
        NULL
      }
    )
  }) |>
    purrr::compact()
}

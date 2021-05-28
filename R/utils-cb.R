#' Industry Classification
#' 
#' @description 
#' For our membership tracking and development
#' strategies, it is extremely useful to map
#' companies to internal platforms. We martch platforms
#' to the industry categories provided by CB.
#' 
#' @param conn connection to anahita
#' @param data The entire `cb_organizations` table
#' 
#' @export
classify_industry_to_platforms <- function(conn, data){

  cli_alert_success("Matching organisations to platforms")

  matching <- tbl(conn, "industry_platform_classification") %>%
    collect() %>%
    separate_rows(Industries, sep = ", ") %>% 
    filter(!is.na(`Platform 1`)) %>% 
    select(category_groups_list = Industries, platform = `Platform 1`)

  categories <- data %>% 
    select(uuid, category_groups_list) %>% 
    separate_rows(category_groups_list, sep = ",")

  df <- categories %>% 
    inner_join(matching, by = "category_groups_list") %>% 
    group_by(uuid) %>% 
    summarise(
      platform = paste0(platform, collapse = "|"),
      .groups = 'drop'
    ) %>% 
    ungroup()

  left_join(data, df, by = "uuid")
  
}

#' Download CB data
#' 
#' @description 
#' For our applications, it is more convenient to
#' apply a mass download of all CB files as CSV's on
#' a daily freqeuncy than built-in live API
#' calls. In the future, we would like to combine our
#' services with live API calls.
#' 
#' @param token CB api token
#' 
#' @export
cb_download_zip <- function(token){
  
  # temp files
  tempin <- tempfile(fileext = ".tar.gz")
  tempout <- tempdir()

  # delete on done or error
  on.exit({
    unlink(tempin, force = TRUE)
  })

  # download data
  cli_alert_info("Downloading Crunchbase Data")
  url <- sprintf("https://api.crunchbase.com/bulk/v4/bulk_export.tar.gz?user_key=%s", token)
  tryCatch(download.file(url, tempin), error = function(e) cli_alert_danger("Failed to download Crunchbase data"))
  cli_alert_success("Downloaded Crunchbase data")

  # untar
  cli_process_start("Unpacking data", "Unpacked data", "Failed to unpack data")
  tryCatch(untar(tempin, exdir = tempout), error = function(e) cli_process_failed())
  cli_process_done()

  invisible(tempout)
}

#' Push Crunchbase Data
#' 
#' @description 
#' Loop over dumped CB files and process data for writing in
#' remote DB.
#' 
#' @param conn connection to the anahita DB
#' @param dir Temp directory as returned by [cb_download_zip()]
#' 
#' @export
cb_push_data <- function(conn, dir){
  
  on.exit({
    unlink(dir, force = TRUE, recursive = TRUE)
  })

  # list files
  files <- list.files(dir, pattern = "*.csv")

  # remove useless and sensitive files
  files <- files[!files %in% c("people.csv", "people_descriptions.csv")]

  for (file in files) {

    # construct path and table name
    path <- sprintf("%s/%s", dir, file)
    table_name <- gsub("\\.csv", "", file)
    table_name <- sprintf("cb_%s", table_name)

    cli_h2("{table_name}")

    # read csv
    cli_process_start("Reading {table_name}", "Read {table_name}", "Failed to read {table_name}")
    df <- tryCatch(read_csv(path, col_types = cols(), progress = FALSE), error = function(e) cli_process_failed())
    cli_process_done()
    
    # modify uuids
    df <- mutate_at(df, vars(contains("uuid")), function(x) gsub("-", "", x))

    # classify platoforms
    if(table_name == "cb_organizations")
      df <- classify_industry_to_platforms(conn, df)

    # write to database
    cli_process_start("Pushing {table_name}", "Pushed {.val {nrow(df)}} records to {table_name}", "Failed to push {table_name} to database")
    tryCatch(dbWriteTable(conn, table_name, df, overwrite = TRUE), error = function(e) cli_process_failed())
    cli_process_done()

    cat("\n")

  }
  
  invisible()
}
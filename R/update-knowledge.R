
#' Update knowledge articles
#' 
#' @description 
#' We populated a table of articles processed by Serendipity in their
#' backennd. It served to us by a endpoint maintained by Serendipity.
#' Provides many information related to moderations and classification.
#' 
#' @param dev deployment status read from config (dev will not right into DBs)
#' @param envr environment (PROD, QA, STAGING)
#' @param time_since starting time for last modification article time
#' @param api_token knowledge endpoint api token
#' 
#' @examples 
#' \dontrun{
#' knwl_update_articles("dev", "QA")
#' }
#' 
#' @return a list of tibbles
#' 
#' @export
knwl_update_articles <- function(dev = config_get(deploy_status), envr = "QA", time_since = NULL, api_token = config_get(api_key_neo4j_qa)) {

  cat("\n\n########### Updating Articles ###########\n")
  cat("---> Deployment status = ", dev, "\n\n")

  con_an <- connect_anahita()

  on.exit({
    cat("Closing connections")
    DBI::dbDisconnect(con_an)
    add = TRUE
  })
    
  my_tbl <- knwl_set_table_name(envr)
  
  cat("Updating knowledge feed: environment = ", envr ," --> SI table = ", my_tbl, "\n")
  
  cat("1. Retrieving all article ids\n")
  
  lst_ids <- tbl(con_an, my_tbl) %>%
    select(a_articleid) %>%
    collect()
  
  cat("2. Calculating last updated time stamp\n")
  
  if (!is.null(time_since)) {
    time_start <- as.numeric(as.POSIXct(time_since))
  } else {
    time <- tbl(con_an, my_tbl) %>%
      select(a_time_analyzed) %>%
      filter(a_time_analyzed == max(a_time_analyzed, na.rm = TRUE)) %>%
      collect() 
  
    time_start <- as.numeric(as.POSIXct(time$a_time_analyzed[1]))
  }
  
  cat("Will start dump from: ", as.character(as.POSIXct(time_start, origin = "1970-01-01 00:00:00")), "\n")
  
  cat("2. Retrieve articles from Neo4j.\n\n")
  
  # this call can take a long time while looping over all article by increments set by LIMIT_ARTICLES (50)
  # I need to re-factor this actually write to the DB after each loop and avoid creating a massive object
  # memory
  
  feed_data <- Rwefsigapi::get_all_articles(time_start - 100, envr, api_token = api_token)
        
  if (!is.null(feed_data)) {  

    cat(paste0("---> article retrieved for ", envr, " endpoint = ", nrow(feed_data), "\n"))

    feed_data <- feed_data %>%
      mutate(a_moderators = as.character(a_moderators)) %>%
      mutate(a_edited_timestamps = as.character(a_edited_timestamps)) %>%
      mutate(s_active = as.character(s_active))
  
    case_1 <- feed_data %>%
      filter(a_articleid %in% lst_ids$a_articleid)
  
    case_2 <- feed_data %>%
      anti_join(lst_ids, by = "a_articleid")

    if (dev == "prod") {
      cat("---> Pushing table to server\n") 
      cat("---> Updating ", nrow(case_1), "articles\n")
      cat("---> Writing ", nrow(case_2), "new articles\n")

      if (nrow(case_1) > 0)
        dbx::dbxUpdate(con_an, my_tbl, case_1, where_cols = c("a_articleid"))
    
      if (nrow(case_2) > 0)
        dbx::dbxInsert(con_an, my_tbl, case_2)
      }
  } else {
    cat(paste0("---> No articles found for ", envr, " endpoint\n"))
  }

  return(feed_data)
  
}


#' Update knowledge articles
#' 
#' @description 
#' We populated a table of articles processed by Serendipity in their
#' backennd. It served to us by a endpoint maintained by Serendipity.
#' Provides many information related to moderations and classification.
#' 
#' @param dev deployment status read from config (dev will not right into DBs)
#' @param api_token knowledge endpoint api token
#' 
#' @examples 
#' \dontrun{
#' knwl_update_sources("dev")
#' }
#' 
#' @return a list of tibbles
#' 
#' @export
knwl_update_sources <- function(dev = config_get(deploy_status), api_token = config_get("api_key_neo4j")) {

  cat("\n\n########### Updating Sources ###########\n")
  cat("---> Deployment status = ", dev, "\n\n")

  con_an <- connect_anahita()

  on.exit({
    cat("Closing connections")
    DBI::dbDisconnect(con_an)
    add = TRUE
  })

  cat("1. Collect source status table\n")
  src_status_old <- tbl(con_an, "knwl_sources_status") %>% 
    collect()

  cat("2. Retrieve all sources from Serendipty\n")
  sources <- Rwefsigapi::get_all_sources("PROD", api_token)

  if (dev == "prod") {
    cat("---> Pushing table to server\n")
    dbWriteTable(con_an, "knwl_sources", sources, overwrite = TRUE)
  }

  # Update source status table
  
  cat("3. Logic for updating source table\n")

  src_status_new <- sources %>%
    select(c("s_id","s_name","s_last_started_update","s_active","s_auto_approved")) %>%
    mutate(time_query = Sys.time())

  src_sources <- src_status_old %>%
    full_join(select(src_status_new, c("s_id","s_name","s_active","s_auto_approved")), by = "s_id") %>%
    mutate(test = (s_name.x == s_name.y & s_active.x == s_active.y & s_auto_approved.x == s_auto_approved.y)) %>%
    filter(test == FALSE | is.na(test))

  if (nrow(src_sources) > 0) {
    
    updates <- src_status_new %>%
      filter(s_id %in% src_sources$s_id) %>%
      select(c("s_id","s_name","s_last_started_update","s_active","s_auto_approved")) %>%
      mutate(time_query = Sys.time())
    
    cat(paste0("---> sources updated: ", nrow(src_sources), "\n"))

    if (dev == "prod") {
      cat("---> Pushing table to server\n")
      dbWriteTable(con_an, "knwl_sources_status", updates, overwrite = FALSE, append = TRUE)
    }
  }

  cat(as.character(Sys.time()), "- all sources", nrow(sources), "- updated sources", nrow(src_sources),"\n")

  return(sources)

}

#' Weekly moderation stats
#' 
#' @description 
#' Weekly moderation targets are based on achieving a number of
#' targets based on total volume of articles but also number of 
#' approved articles per topic and diversity of sources per topic
#' among the approved articles.
#' 
#' @param dev deployment status read from config (dev will not right into DBs)
#' @param unit_time set to week (can be day, month, year)
#' @param unit_back set 56 weeks back
#' @param week_starts set -1 (starts Saturday)
#' 
#' @examples
#' \dontrun{
#' con_an <- connect_anahita()
#' dt <- knwl_update_weekly_moderation_targets(con_an)
#' }
#' 
#' @return a tibble
#' 
#' @importFrom lubridate ceiling_date floor_date
#' 
#' @export
knwl_update_weekly_moderation_targets <- function(
  dev = config_get(deploy_status),
  unit_time = "week",
  unit_back = 56,
  week_starts = -1) {

  cat("\n\n########### Update weekly moderation ###########\n")
  cat("---> Deployment status = ", dev, "\n\n")

  con_an <- connect_anahita()

  on.exit({
    cat("Closing connections")
    DBI::dbDisconnect(con_an)
    add = TRUE
    })

  lst_idx <- unit_back:0

  cat("1. Calculating Stats\n")

  dt <- do.call(
    bind_rows,
    lapply(
      lst_idx,
      function(x) {

        if (unit_time == "week") {
          t1 = floor_date(Sys.Date()-7*x, unit = unit_time, week_start = week_starts)
          t2 = ceiling_date(Sys.Date()-7*x, unit = unit_time, week_start = week_starts+7)
        } else {
          t1 = floor_date(Sys.Date()-7*x, unit = unit_time)
          t2 = ceiling_date(Sys.Date()-7*x, unit = unit_time)
        }
        
        tibble(start_date = t1, end_date = t2) %>%
          bind_cols(knwl_moderation_summary(con_an, t1, t2)[[1]])
      }
    ))

  if (dev == "prod") {
    cat("2. Pushing table to server\n")
    dbWriteTable(con_an, "knwl_weekly_moderation_stats", dt, overwrite = TRUE)
  }

  return(dt)

}

#' Outer ring stats
#' 
#' @description 
#' We calculate the volume of articles co-occuring accross pairs
#' of topics.
#' 
#' @param dev deployment status read from config (dev will not right into DBs)
#' @param time_since start time (default NULL)
#' @param time_to end time (default is one year back)
#' 
#' @examples
#' \dontrun{
#' con_an <- connect_anahita()
#' dt <- knwl_update_weekly_moderation_targets(con_an)
#' }
#' 
#' @return a tibble
#' 
#' @export
knwl_update_outer_ring_stats <- function(dev = config_get(deploy_status), time_since = Sys.time()-3600*24*365, time_to = NULL) {

  cat("\n\n########### Outer Ring Stats ###########\n")
  cat("---> Deployment status = ", dev, "\n\n")

  con_an <- connect_anahita()

  on.exit({
    cat("Closing connections")
    DBI::dbDisconnect(con_an)
    add = TRUE
    })

  cat("1. Calculating Stats\n")
  dt <- knwl_outer_ring_stats(con_an, time_since, time_to)

  if (dev == "prod") {
    cat("2. Pushing table to server\n")
    dbWriteTable(con_an, "knwl_outer_ring_volume_coverage", dt, overwrite = TRUE)
  }

  return(dt)
}

#' Weekly moderation stats
#' 
#' @description 
#' Weekly moderation targets are based on achieving a number of
#' targets based on total volume of articles but also number of 
#' approved articles per topic and diversity of sources per topic
#' among the approved articles.
#' 
#' @param dev deployment status read from config (dev will not right into DBs)
#' @param dirpath path to save local copy of file
#' 
#' @examples 
#' \dontrun{
#' con_an <- connect_anahita()
#' dt <- knwl_update_tpk_network("./")
#' }
#' 
#' @return a tibble
#' 
#' @export
knwl_update_tpk_network <- function(dev = config_get(deploy_status), dirpath = NULL) {

  cat("\n\n########### Updating TM Network file ###########\n")
  cat("---> Deployment status = ", dev, "\n\n")

  con_an <- connect_anahita()

  on.exit({
    cat("Closing connections")
    DBI::dbDisconnect(con_an)
    add = TRUE
    })

  df_net <- knwl_toplink_network()
  
  if (dev == "prod") {
    cat("2. Pushing table to server\n")
    dbWriteTable(con_an, "knwl_tm_network", df_net, overwrite = TRUE)
  }

  if (!is.null(dirpath)) {
    dir <- paste0(dirpath, format(Sys.time(), "%Y%m%d_%H%M"), "_tm_network.csv")
    cat("2. Saving file at", dir, "\n")
    readr::write_excel_csv(df_net, dir)
  }

  return(df_net)

}
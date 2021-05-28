#' Update mixpanel events and views
#' 
#' @description 
#' This daily automation crawls event data from the serendipity and
#' toplink mixpanel accounts, append to existing tables and
#' updates a number of daily views, mainly around tracking conversions
#' and dedicated events in the platform.
#' 
#' @param time_since starting time (default is yesterday)
#' @param time_to ending time (default is yesterday)
#' @param dev deployment status read from config (dev will not right into DBs)
#' @param key_sry mixpanel serendipidity API secret
#' @param key_tpk mixpanel toplink API secret
#' 
#' @examples 
#' \dontrun{
#' update_mixpanel_and_views()
#' }
#' 
#' @return a list of tibbles
#' 
#' @export
update_mixpanel_and_views <- function(
  time_since = Sys.Date() - 1, 
  time_to = Sys.Date() - 1,
  dev = config_get(deploy_status),
  key_sry = config_get(mp_si),
  key_tpk = config_get(mp_dm)) {

  cat("\n\n########### Update mixpanel and all views ###########\n")
  cat("---> Deployment status = ", dev, "\n\n")

  #con_mp <- connect_mixpanel()
  #con_dm <- connect_dm()
  con_an <- connect_anahita()

  on.exit({
    cat("Closing connections")
    #DBI::dbDisconnect(con_mp)
    #DBI::dbDisconnect(con_dm)
    DBI::dbDisconnect(con_an)
    add = TRUE
    })

  cat("0. Preparing the Data\n")
  users <- sf_build_user_emails_to_id(con_an)
  stripe <- stripe_customers(con_an)

  cat("1. Retrieving Mixpanel Serendipity Data\n")
  dir <- "./mpdata"
  mxp_set_up_dir(dir)
  mixpanel_data <- mxp_fetch_data(time_since, time_to, key_sry, mixpanel_columns_sry, dir)
  mxp_clean_dir(dir)
  mixpanel_data_clean <- mxp_clean_events(con_an, mixpanel_data, users, "sry")

  mixpanel_data_dms <- mixpanel_data_clean %>%
    filter(distinct_id %in% stripe$id) %>%
    select(dplyr::one_of(mixpanel_columns_dms)) %>%
    rename(
      id = "distinct_id",
      entity_name = "entityName",
      entity_type = "entityType",
      secondary_entity_type = "secondaryEntityType",
      country_code = "mp_country_code",
      entity_id = "entityId",
      secondary_entity_id = "secondaryEntityId",
      secondary_entity_name = "secondaryEntityName"
    )

  cat("---> 1.1 All users data: found", nrow(mixpanel_data), "events\n")
  cat("---> 1.2 Dedicated paying DM view: found", nrow(mixpanel_data_dms), "events\n")

  cat("2. Pulling data from TopLink\n")
  dir <- "./dmdata"
  mxp_set_up_dir(dir)
  mixpanel_data_toplink <- mxp_fetch_data(time_since, time_to, key_tpk, mixpanel_columns_tpk, dir)
  mxp_clean_dir(dir)
  mixpanel_data_toplink <- mxp_clean_events(con_an, mixpanel_data_toplink, users, "tpk")
  cat("---> 2.1 All users / found", nrow(mixpanel_data_toplink), "events\n")

  if (dev == "prod") {

    cat("Pushing table to server\n")

    mixpanel_data_clean_original <- mixpanel_data_clean %>%
      select(-topic_id_url)

    #dbWriteTable(con_mp, "events", mixpanel_data, append = TRUE, overwrite = FALSE)
    #dbWriteTable(con_mp, "events_clean", mixpanel_data_clean_original, append = TRUE, overwrite = FALSE)
    #dbWriteTable(con_dm, "events", mixpanel_data_dms, append = TRUE, overwrite = FALSE)  
    #dbWriteTable(con_dm, "digital_members_events", mixpanel_data_toplink, append = TRUE, overwrite = FALSE)

    dbWriteTable(con_an, "events", mixpanel_data, append = TRUE, overwrite = FALSE)
    dbWriteTable(con_an, "events_clean", mixpanel_data_clean_original, append = TRUE, overwrite = FALSE)
    dbWriteTable(con_an, "mxp_sry_events_dms", mixpanel_data_dms, append = TRUE, overwrite = FALSE)
    dbWriteTable(con_an, "mxp_tpk_events_dms", mixpanel_data_toplink, append = TRUE, overwrite = FALSE)

    is_second_day_of_month <- ifelse(format(Sys.Date(), "%d") == "02", TRUE, FALSE)

    if (is_second_day_of_month) {
      cat("1.2 Second day of the month, overwriting monthly views\n")
      dbWriteTable(con_an, "mxp_sry_events_monthly", mixpanel_data, overwrite = TRUE)
      dbWriteTable(con_an, "mxp_sry_events_clean_monthly", mixpanel_data_clean, overwrite = TRUE)
      dbWriteTable(con_an, "mxp_sry_events_dms_monthly", mixpanel_data_dms, overwrite = TRUE)
      dbWriteTable(con_an, "mxp_tpk_events_dms_monthly", mixpanel_data_toplink, overwrite = TRUE)
    } else {
      cat("1.2 Not the second day of the month, appending to the monthly views\n")
      dbWriteTable(con_an, "mxp_sry_events_monthly", mixpanel_data, append = TRUE, overwrite = FALSE)
      dbWriteTable(con_an, "mxp_sry_events_clean_monthly", mixpanel_data_clean, append = TRUE, overwrite = FALSE)
      dbWriteTable(con_an, "mxp_sry_events_dms_monthly", mixpanel_data_dms, append = TRUE, overwrite = FALSE)
      dbWriteTable(con_an, "mxp_tpk_events_dms_monthly", mixpanel_data_toplink, append = TRUE, overwrite = FALSE)
    }
  }

  cat("Building views...\n")

  mixpanel_data_clean <- mixpanel_data_clean %>%
    arrange(desc(time)) %>%
    mutate(date = as.Date(time))

  mixpanel_data_toplink <- mixpanel_data_toplink %>%
      mutate(date = as.Date(time))

  cat("3. Build membership conversion tables\n")
  conversions <- build_conversion_tables(stripe, mixpanel_data_clean, mixpanel_data_toplink)

  if (dev == "prod") {
    cat("----> Pushing table to server\n")
    dbWriteTable(con_an, "mxp_sry_upgrade_utm", conversions$upgrade_utm, append = TRUE, overwrite = FALSE)
    dbWriteTable(con_an, "mxp_tpk_payment_portal", conversions$payment_page, append = TRUE, overwrite = FALSE)
    dbWriteTable(con_an, "dms_signups", conversions$sry_utm_upgrades, append = TRUE, overwrite = FALSE)
    dbWriteTable(con_an, "dms_abandon_basket", conversions$abandoned_utm, append = TRUE, overwrite = FALSE)
  }

  cat("4. Build conversion funnel tables\n")
  funnel <- build_conversion_funnel(stripe, mixpanel_data_clean, mixpanel_data_toplink)

  if (dev == "prod") {
    cat("----> Pushing table to server\n")
    dbWriteTable(con_an, "si_users_funnels", funnel, append = TRUE, overwrite = FALSE)
  }

  cat("5. Build Forum domain logins tables\n") 
  domain_logins <- mxp_track_weforum_logins(mixpanel_data_clean)

  if (dev == "prod") {
    cat("----> Pushing table to server\n")
    dbWriteTable(con_an, "mxp_sry_weforum_signups", domain_logins$weforum_signups, append = TRUE, overwrite = FALSE)
    dbWriteTable(con_an, "mxp_sry_weforum_logins", domain_logins$weforum_logins, append = TRUE, overwrite = FALSE)
  }

  cat("6. SI activity event collection\n")
  cat("6.1 Custom map events collection\n")
  events_custom_map <- build_custom_map_table(mixpanel_data_clean)
  cat("6.2 Earthtime event collection\n")
  events_earthtime <- build_earthtime_table(mixpanel_data_clean)
  cat("6.3 Monitor event collection\n")
  events_monitor <- build_monitor_table(mixpanel_data_clean)
  cat("6.4 Advanced Analytics\n")
  events_advanced_analytics <- build_advanced_analytics_table(mixpanel_data_clean)
  cat("6.4 Briefing event collection\n")
  events_briefings <- build_briefings_table(mixpanel_data_clean)
  cat("6.5 Topic event collection\n")
  events_insights <- build_insights_table(mixpanel_data_clean)
  cat("6.6 Key Issue event collection\n")
  events_issues <- build_issues_table(mixpanel_data_clean)
  cat("6.7 Articles event collection\n")
  events_articles <- build_articles_table(mixpanel_data_clean)

  if (dev == "prod") {
    cat("----> Pushing table to server\n")
    dbWriteTable(con_an, "mxp_sry_custom_maps", events_custom_map, append = TRUE, overwrite = FALSE)
    dbWriteTable(con_an, "mxp_sry_earthtime", events_earthtime, append = TRUE, overwrite = FALSE)
    dbWriteTable(con_an, "mxp_sry_monitor", events_monitor, append = TRUE, overwrite = FALSE)
    dbWriteTable(con_an, "mxp_sry_analytics", events_advanced_analytics, append = TRUE, overwrite = FALSE)
    dbWriteTable(con_an, "mxp_sry_briefings", events_briefings, append = TRUE, overwrite = FALSE)
    dbWriteTable(con_an, "mxp_sry_topics", events_insights, append = TRUE, overwrite = FALSE)
    dbWriteTable(con_an, "mxp_sry_issues", events_issues, append = TRUE, overwrite = FALSE)
    dbWriteTable(con_an, "mxp_sry_articles", events_articles, append = TRUE, overwrite = FALSE)
  }

  return(list(
    "mxp_sry_events" = mixpanel_data_clean,
    "mxp_tpk_events_dms" = mixpanel_data_toplink))

  cat("\n\n########### Update procedure completed ###########\n")
}

#' Initialize views
#' 
#' @description
#' Using a massive dump of the mixpanel events table, one
#' creates clean views of relevant tables from scratch.
#' 
#' @param f1 serendipity event dump file location (if not provided queries DB)
#' @param f2 toplink event dump file location (if not provided queries DB)
#' @param dev deployment status (dev does not push tables) 
#' 
#' @examples 
#' \dontrun{
#' conn1 <- connect_dw()
#' conn2 <- connect_anahita()
#' update_si_stats(conn1, conn2)
#' }
#' 
#' @return a tibble
#' 
#' @export
initialize_all_views <- function(f1 = NULL, f2 = NULL, dev = config_get(deploy_status)) {

  cat("\n\n########### Initialize all views ###########\n")
  cat("---> Deployment status = ", dev, "\n\n")

  con_an <- connect_anahita()

  on.exit({
      cat("Closing connections")
      DBI::dbDisconnect(con_an)
      add = TRUE
    })

  cat("0. Preparing the Data\n")
  users <- sf_build_user_emails_to_id(con_an)
  stripe <- stripe_customers(con_an)

  cat("1. Retrieving Mixpanel Serendipity Data\n")
  #all_users <- mxp_get_sry_table(con_mp, f1)
  all_users <- mxp_get_sry_table(con_an, f1)
  all_users <- mxp_clean_events(con_an, all_users, users, "sry")

  cat("2. Retrieving Mixpanel Toplink Data\n")
  #all_users_tpk <- mxp_get_tpk_table(con_mp, f2)
  all_users_tpk <- mxp_get_tpk_table(con_an, f2)
  all_users_tpk <- mxp_clean_events(con_an, all_users_tpk, users, "tpk")
  
  cat("3. Build membership conversion tables\n")
  conversions <- build_conversion_tables(stripe, all_users, all_users_tpk)

  if (dev == "prod") {
    cat("----> Pushing table to server\n")
    dbWriteTable(con_an, "mxp_sry_upgrade_utm", conversions$upgrade_utm, overwrite = TRUE)
    dbWriteTable(con_an, "mxp_tpk_payment_portal", conversions$payment_page, overwrite = TRUE)
    dbWriteTable(con_an, "dms_signups", conversions$sry_utm_upgrades, overwrite = TRUE)
    dbWriteTable(con_an, "dms_abandon_basket", conversions$abandoned_utm, overwrite = TRUE)
  }

  cat("4. Build conversion funnel tables\n")
  funnel <- build_conversion_funnel(stripe, all_users, all_users_tpk)

  if (dev == "prod") {
    cat("----> Pushing table to server\n")
    dbWriteTable(con_an, "si_users_funnels", funnel, overwrite = TRUE)
  }

  cat("5. Build Forum domain logins tables\n") 
  domain_logins <- mxp_track_weforum_logins(all_users)

  if (dev == "prod") {
    cat("----> Pushing table to server\n")
    dbWriteTable(con_an, "mxp_sry_weforum_signups", domain_logins$weforum_signups, overwrite = TRUE)
    dbWriteTable(con_an, "mxp_sry_weforum_logins", domain_logins$weforum_logins, overwrite = TRUE)
  }

  cat("6. SI activity event collection\n")
  cat("6.1 Custom map events collection\n")
  events_custom_map <- build_custom_map_table(all_users)
  cat("6.2 Earthtime event collection\n")
  events_earthtime <- build_earthtime_table(all_users)
  cat("6.3 Monitor event collection\n")
  events_monitor <- build_monitor_table(all_users)
  cat("6.4 Advanced Analytics\n")
  events_advanced_analytics <- build_advanced_analytics_table(all_users)
  cat("6.4 Briefing event collection\n")
  events_briefings <- build_briefings_table(all_users)
  cat("6.5 Topic event collection\n")
  events_insights <- build_insights_table(all_users)
  cat("6.6 Key Issue event collection\n")
  events_issues <- build_issues_table(all_users)
  cat("6.7 Articles event collection\n")
  events_articles <- build_articles_table(all_users)

  if (dev == "prod") {
    cat("----> Pushing table to server\n")
    dbWriteTable(con_an, "mxp_sry_custom_maps", events_custom_map, overwrite = TRUE)
    dbWriteTable(con_an, "mxp_sry_earthtime", events_earthtime, overwrite = TRUE)
    dbWriteTable(con_an, "mxp_sry_monitor", events_monitor, overwrite = TRUE)
    dbWriteTable(con_an, "mxp_sry_analytics", events_advanced_analytics, overwrite = TRUE)
    dbWriteTable(con_an, "mxp_sry_briefings", events_briefings, overwrite = TRUE)
    dbWriteTable(con_an, "mxp_sry_topics", events_insights, overwrite = TRUE)
    dbWriteTable(con_an, "mxp_sry_issues", events_issues, overwrite = TRUE)
    dbWriteTable(con_an, "mxp_sry_articles", events_articles, overwrite = TRUE)
  }

  return(events_issues)

  cat("\n\n########### Initialization completed ###########\n")
}


#' Initialize monthly views
#' 
#' @description
#' Start monthly views from scratch using a time
#' query to mixpanel events table.
#' 
#' @param dev deployment status (dev does not push tables) 
#' 
#' @examples 
#' \dontrun{
#' initialize_all_monthly_views("dev")
#' }
#' 
#' @return a tibble
#' 
#' @export
initialize_all_monthly_views <- function(dev = config_get(deploy_status)) {

  cat("\n\n########### Initialize all views ###########\n")
  cat("---> Deployment status = ", dev, "\n\n")

  #con_mp <- connect_mixpanel()
  #con_dm <- connect_dm()
  con_an <- connect_anahita()

  on.exit({
      cat("Closing connections")
      #DBI::dbDisconnect(con_mp)
      #DBI::dbDisconnect(con_dm)
      DBI::dbDisconnect(con_an)
      add = TRUE
    })

  cat("0. Preparing the Data\n")
  users <- sf_build_user_emails_to_id(con_an)
  stripe <- stripe_customers(con_an)

  cat("1. Retrieving Mixpanel Serendipity Data\n")

  time_since <- lubridate::floor_date(lubridate::ymd(Sys.Date()), 'month')
  time_to <- Sys.Date()

  cat("1.1 Pulling data from", as.character(time_since), "to", as.character(time_to), "\n")

  #mixpanel_data <- tbl(con_mp, "events") %>%
  mixpanel_data <- tbl(con_an, "events") %>%
    filter(time >= time_since & time <= time_to) %>%
    collect()

  cat("1.2 Cleaning events\n")

  mixpanel_data_clean <- mxp_clean_events(con_an, mixpanel_data, users, "sry")

  cat("1.3 Subsetting paying DMs\n")

  mixpanel_data_dms <- mixpanel_data_clean %>%
    filter(distinct_id %in% stripe$id) %>%
    dplyr::select(dplyr::one_of(mixpanel_columns_dms)) %>%
    dplyr::rename(
      id = "distinct_id",
      entity_name = "entityName",
      entity_type = "entityType",
      secondary_entity_type = "secondaryEntityType",
      country_code = "mp_country_code",
      entity_id = "entityId",
      secondary_entity_id = "secondaryEntityId",
      secondary_entity_name = "secondaryEntityName"
    )

  cat("2. Retrieving Mixpanel Toplink Data\n")
  cat("2.1 Pulling data from", as.character(time_since), "to", as.character(time_to), "\n")

  #mixpanel_data_toplink <- tbl(con_dm, "digital_members_events") %>%
  mixpanel_data_toplink <- tbl(con_an, "mxp_tpk_events_dms") %>%
    filter(time >= time_since & time <= time_to) %>%
    collect()

  cat("2.2 Cleaning events\n")
  mixpanel_data_toplink <- mxp_clean_events(con_an, mixpanel_data_toplink, users, "tpk")

  # save tables

  cat("3. Saving monthly views in anahita \n")

  if (dev == "prod") {
    cat("----> Pushing table to server\n")
    dbWriteTable(con_an, "mxp_sry_events_monthly", mixpanel_data, overwrite = TRUE)
    dbWriteTable(con_an, "mxp_sry_events_clean_monthly", mixpanel_data_clean, overwrite = TRUE)
    dbWriteTable(con_an, "mxp_sry_events_dms_monthly", mixpanel_data_dms, overwrite = TRUE)
    dbWriteTable(con_an, "mxp_tpk_events_dms_monthly", mixpanel_data_toplink, overwrite = TRUE)
  }

  return(list(
    "mxp_sry_events_monthly" = mixpanel_data,
    "mxp_sry_events_clean_monthly" = mixpanel_data_clean,
    "mxp_sry_events_dms_monthly" = mixpanel_data_dms,
    "mxp_tpk_events_dms_monthly" = mixpanel_data_toplink)
    )

  cat("\n\n########### Initialization completed ###########\n")
}
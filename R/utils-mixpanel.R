


#' Set-up download dir
#' @param dir name
#' @export
mxp_set_up_dir <- function(dir){
  if(dir.exists(dir))
    mxp_clean_dir(dir)

  cat("Creating directory.\n")
  dir.create(dir) # create temp

  stopifnot(dir.exists(dir))
  invisible()
}

#' Clean download dir
#' @param dir name
#' @export
mxp_clean_dir <- function(dir){
  cat("Deleting directory.\n")
  unlink(dir, TRUE, TRUE) # delete temp
}

#' Pre-process mxp data
#' @param data a mixpanel tibble
#' @export
mxp_preprocess <- function(data){
  data %>% 
  mutate(
    nchar = nchar(distinct_id),
    time = mixpanel::convert_timestamp(time),
    user_type = case_when(
      nchar == 18 ~ "Salesforce",
      grepl("@", distinct_id) ~ "email",
      grepl("anonymous", distinct_id) ~ "Mobile Anonymous",
      TRUE ~ "Web Anonymous"
    )
  )
}

#' Fetch mixpanel data
#' 
#' @description 
#' Will use the mixpanel export API to dump files in local temp dir 
#' and then read back in the JSON.
#'
#' @param from_date start date
#' @param to_date end date
#' @param api_secret mixpanel API
#' @param col_sels vector of columns to select
#' @param dir temp dir location
#'
#' @examples
#' \dontrun{
#' t1 = Sys.Date() - 1
#' t2 = Sys.Date() - 1
#' api <- config_get(mp_si)
#' cols <- c("time", "distinct_id")
#' dir <- "./mpdata"
#' mxp_set_up_dir(dir)
#' df <- mxp_fetch_data(t1, t2, api, cols, dir)
#' }
#' 
#' @return a tibble
#' 
#' @export
mxp_fetch_data <- function(from_date, to_date, api_secret, col_sels, dir) {

  if (is.null(api_secret))
    stop("Mixpanel API secret is NULL")

  mixpanel::get_mixpanel_data(
    from_date = from_date,
    to_date = to_date,
    api_secret = api_secret) %>% 
    mixpanel::parse_mixpanel_data(dir = dir)

  mixpanel_data <- mixpanel::mixpanel_data_from_json_dir(dir)

  mixpanel_data %>% 
    mxp_preprocess() %>%
    select(dplyr::one_of(col_sels)) %>%
    as_tibble()


}

#' Replace emails by ids
#' 
#' @description 
#' Utility function that replaces email identifiers by 
#' their associated SF id.
#'
#' @param dt a mixpanel events table
#' @param tbl_users table of emails to id mapping (see `mxp_build_user_ids_emails`)
#'
#' @examples
#' \dontrun{
#' conn1 <- connect_anahita()
#' dt1 <- fetch_mixpanel_data()
#' dt2 <- mxp_build_user_ids_emails(conn1)
#' df <- mxp_fix_ids(dt1, dt2)
#' }
#' 
#' @return a tibble
#' 
#' @export
mxp_fix_ids <- function(dt, tbl_users) {

  dt %>%
    mutate(distinct_id = ifelse(grepl("@", distinct_id), tolower(distinct_id), distinct_id)) %>%
    left_join(tbl_users, by = c("distinct_id" = "toplink_username__c")) %>%
    mutate(distinct_id = ifelse((nchar(distinct_id) != 18 | grepl("@", distinct_id)) & !is.na(id), id, distinct_id)) %>%
    select(-id)

}

#' Fix earthtime story names
#' 
#' @description 
#' Earthtime events are not properly recognized. We need to
#' use story names to tag these events as earthtime events
#' for easier manipulation.
#'
#' @param conn1 connection to anahita DB
#' @param dt a mixpanel events table
#'
#' @examples
#' \dontrun{
#' conn1 <- connect_anahita()
#' dt <- fetch_mixpanel_data()
#' df <- mxp_fix_earthtime_story_names(conn1, dt)
#' }
#' 
#' @return a tibble
#' 
#' @export
mxp_fix_earthtime_story_names <- function(conn1, dt) {

  earthtime <- tbl(conn1, "knwl_earthtime_stories") %>% 
    collect() %>%
    pull(story_name)

  dt %>%
    mutate(event = ifelse(
      (event == 'Open Link' & client == "web-app" & page == "Topic detail" & subpage == "Data" & entityName %in% earthtime) | 
      (event == 'Browse page' & client == "mobile-app" & page == "Data detail" & is.na(subpage) & entityName %in% earthtime),
      "earthtime",
      event))

}

#' Enrich topic ids
#' 
#' @description 
#' Certain events can be associated to browsing a given topic
#' but mixpanel will register the id related to the specific event.
#' We create a new topic id to enrich with the topic id found in the
#' URL.
#'
#' @param dt a mixpanel events table
#'
#' @examples
#' \dontrun{
#' dt <- fetch_mixpanel_data()
#' df <- mxp_fix_topic_ids(dt)
#' }
#' 
#' @return a tibble
#' 
#' @importFrom stringr str_match
#' 
#' @export
mxp_fix_topic_ids <- function(dt) {

  dt %>%
    mutate(topic_id_url = str_match(current_url, "/topics/\\s*(.*?)\\s*\\?tab")[,2]) %>%
    mutate(entityId = ifelse(is.na(entityId) & !is.na(topic_id_url), topic_id_url, entityId)) %>%
    mutate(topic_id_url = ifelse(is.na(topic_id_url) & !is.na(entityId) & nchar(entityId) == 18, entityId, topic_id_url))

}

#' Serendipity Mixpanel Events
#' 
#' @description 
#' Load or dump events data. The dump could last several
#' hours.
#'
#' @param conn1 a connection to anahita DB
#' @param fpath path to dumped file if exsists
#'
#' @examples
#' \dontrun{
#' conn1 <- connect_anahita()
#' df <- mxp_get_sry_table(conn1)
#' }
#' 
#' @return a tibble
#' 
#' @importFrom stringr str_match
#' 
#' @export
mxp_get_sry_table <- function(conn1, fpath) {

  if (!is.null(fpath)) {
    cat("---> Loading mixpanel events data\n")
    load(fpath)
  } else {
    cat("---> Pulling mixpanel events data\n")

    sel_var <- c(
      "time", "distinct_id", "event", "current_url", "referring_domain", 
      "utm_campaign", "utm_medium", "utm_source", "client", "entityType", 
      "entityId", "entityName", "secondaryEntityId", 
      "secondaryEntityName", "page", "subpage")

    all_users <- tbl(conn1, "events") %>%
      select(all_of(sel_var)) %>%
      collect() %>%
      arrange(desc(time)) %>%
      mutate(date = as.Date(time))
  }

  return(all_users)

}

#' Toplink Mixpanel Events
#' 
#' @description 
#' Load or dump events data. The dump could last several
#' hours.
#'
#' @param conn1 a connection to anahita DB
#' @param fpath path to dumped file if exsists
#'
#' @examples
#' \dontrun{
#' conn1 <- connect_anahita()
#' df <- mxp_get_tpk_table(conn1)
#' }
#' 
#' @return a tibble
#' 
#' @importFrom stringr str_match
#' 
#' @export
mxp_get_tpk_table <- function(conn1, fpath) {

  if (!is.null(fpath)) {
    cat("---> Loading mixpanel toplink events data\n")
    load(fpath)
  } else {
    cat("---> Pulling mixpanel toplink events data\n")
    #all_users_tpk <- tbl(con_dm, "digital_members_events") %>%
    all_users_tpk <- tbl(con_an, "mxp_tpk_events_dms") %>%
      select(time, distinct_id, event, current_url, referring_domain, utm_campaign, utm_medium, utm_source) %>%
      collect() %>%
      mutate(date = as.Date(time))
  }

  return(all_users_tpk)

}

#' Clean mixpanel events
#' 
#' @description 
#' Raw mixpanel event suffer issues in tracking
#' implemention that need to be fixed for cleaner data.
#'
#' @param conn1 a connection to anahita DB
#' @param dt mixpanel events tibble (see `mxp_get_sry_table` or `mxp_get_tpk_table`)
#' @param email_to_ids tibble of email to id mappings (see `sf_build_user_emails_to_id`)
#' @param source mixpanel source, Serendipity (sry) or other (tpk e.g.)
#'
#' @examples
#' \dontrun{
#' conn1 <- connect_anahita()
#' df <- mxp_clean_events(conn1)
#' }
#' 
#' @return a tibble
#' 
#' @importFrom stringr str_match
#' 
#' @export
mxp_clean_events <- function(conn1, dt, email_to_ids, source = "sry") {

  if (source == "sry") {
    cat("---> Fixing ids\n")
    dt <- mxp_fix_ids(dt, email_to_ids)
    cat("---> Fixing earhtime\n")
    dt <- mxp_fix_earthtime_story_names(conn1, dt)
    cat("---> Fixing topic ids\n")
    dt <- mxp_fix_topic_ids(dt)
  } else {
    cat("---> Fixing ids\n")
    dt <- mxp_fix_ids(dt, email_to_ids)
  }

  return(dt)
}

#' Custom maps
#' 
#' @description 
#' Event view for custom map related events only
#'
#' @param events_si tibble of mixpanel events from serendipity (see `mxp_get_sry_table`)
#'
#' @examples
#' \dontrun{
#' df <- build_custom_map_table(events_si)
#' }
#' 
#' @return a tibble
#' 
#' @export
build_custom_map_table <- function(events_si) {

  event_types <- c("Add Topic to Map", "Edit Map", "Create Map", "Clear Map")
  excl_entities <- c("(new map)", "local")

  events_si %>%
    filter(event %in% event_types & !entityName %in% excl_entities) %>%
    select(time, distinct_id, entityName, event) %>%
    collect() %>%
    distinct_all()

}

#' Earthtime stories
#' 
#' @description 
#' Event view for earthtime related events only
#'
#' @param events_si tibble of mixpanel events from serendipity (see `mxp_get_sry_table`)
#'
#' @examples
#' \dontrun{
#' df <- build_earthtime_table(events_si)
#' }
#' 
#' @return a tibble
#' 
#' @export
build_earthtime_table <- function(events_si) {

  event_types <- c("earthtime")

  events_si %>%
    filter(event %in% event_types) %>%
    select(time, distinct_id, entityName, event) %>%
    collect()

}

#' Monitor
#' 
#' @description 
#' Event view for monitor related events only
#'
#' @param events_si tibble of mixpanel events from serendipity (see `mxp_get_sry_table`)
#'
#' @examples
#' \dontrun{
#' df <- build_monitor_table(events_si)
#' }
#' 
#' @return a tibble
#' 
#' @export
build_monitor_table <- function(events_si) {

  events_si %>%
    filter(event == "Open Drawer" | entityType == "Cluster" | grepl("https://intelligence.weforum.org/monitor", current_url)) %>%
    select(time, distinct_id, entityName, event) %>%
    collect()

}

#' Advanced Analytics
#' 
#' @description 
#' Event view for advanced analytics related events only
#'
#' @param events_si tibble of mixpanel events from serendipity (see `mxp_get_sry_table`)
#'
#' @examples
#' \dontrun{
#' df <- build_advanced_analytics_table(events_si)
#' }
#' 
#' @return a tibble
#' 
#' @export
build_advanced_analytics_table <- function(events_si) {

  event_types <- c(
    "Open Advanced Analytics", 
    "Select Advanced Analytics Time Period", 
    "Select Advanced Analytics Secondary Topic")

  events_si %>%
    filter(event %in% event_types) %>%
    select(time, distinct_id, entityName, event) %>%
    collect()

}

#' Briefings
#' 
#' @description 
#' Event view for briefing related events only
#'
#' @param events_si tibble of mixpanel events from serendipity (see `mxp_get_sry_table`)
#'
#' @examples
#' \dontrun{
#' df <- build_briefings_table(events_si)
#' }
#' 
#' @return a tibble
#' 
#' @export
build_briefings_table <- function(events_si) {

 events_si %>%
    filter(event == "Generate briefing") %>%
    select(time, distinct_id, entityName, event) %>%
    collect() %>%
    distinct_all()

}

#' Insights
#' 
#' @description 
#' Event view for insights related events only
#'
#' @param events_si tibble of mixpanel events from serendipity (see `mxp_get_sry_table`)
#'
#' @examples
#' \dontrun{
#' df <- build_insights_table(events_si)
#' }
#' 
#' @return a tibble
#' 
#' @export
build_insights_table <- function(events_si) {

  event_types <- c("Browse page", "Browse Page")

  events_si %>%
    filter(event %in% event_types & entityType == "Topic" & entityName != "") %>%
    select(time, distinct_id, entityId, entityName, secondaryEntityId, secondaryEntityName, event, topic_id_url) %>%
    collect()

}

#' Issues
#' 
#' @description 
#' Event view for issues related events only
#'
#' @param events_si tibble of mixpanel events from serendipity (see `mxp_get_sry_table`)
#'
#' @examples
#' \dontrun{
#' df <- build_issues_table(events_si)
#' }
#' 
#' @return a tibble
#' 
#' @export
build_issues_table <- function(events_si) {

  event_types <- c("Browse page", "Browse Page")

  events_si %>%
    filter(event %in% event_types & entityType == "Topic" & secondaryEntityName != "") %>%
    select(time, distinct_id, entityId, entityName, secondaryEntityId, secondaryEntityName, event) %>%
    collect()

}

#' Articles
#' 
#' @description 
#' Event view for article related events only
#'
#' @param events_si tibble of mixpanel events from serendipity (see `mxp_get_sry_table`)
#'
#' @examples
#' \dontrun{
#' df <- build_articles_table(events_si)
#' }
#' 
#' @return a tibble
#' 
#' @export
build_articles_table <- function(events_si) {

  web_events_si <- events_si %>%
    filter(event == "Open Link") %>%
    filter(subpage == "Publications") %>%
    select(time, distinct_id, entityId, entityName, client) %>%
    mutate(client = ifelse(is.na(client), "web-app", client))

  mobile_events_si <- events_si %>%
    filter(event == "Open link") %>%
    filter(client == "mobile-app") %>%
    select(time, distinct_id, entityId, entityName, client)

  bind_rows(web_events_si, mobile_events_si)
}

#' Membership conversion tracking
#' 
#' @param stripe tibble of stripe customers (see `stripe_customers`)
#' @param events_si tibble of mixpanel events from serendipity (see `mxp_get_sry_table`)
#' @param events_tpk tibble of mixpanel events from toplink (see `mxp_get_sry_table`)
#' 
#' @export
build_conversion_tables <- function(stripe, events_si, events_tpk) {

  ids_converted <- stripe %>% 
    filter(status %in% c("active", "canceled", "past_due", "trialing")) %>% 
    distinct(id) %>%
    pull(id)

  upgrade_utm <- events_si %>%
    filter(!is.na(utm_campaign) & event == "Upgrade") %>%
    select(time, distinct_id, utm_campaign, utm_medium, utm_source, current_url)

  payment_page <- events_tpk %>%
    select(time, distinct_id, utm_campaign, utm_medium, utm_source, current_url) %>%
    filter(grepl("https://digital-members.weforum.org/intelligence/payment", current_url))

  sry_utm_upgrades <- upgrade_utm %>%
    filter(distinct_id %in% ids_converted) %>%
    select(id = "distinct_id", utm_campaign, intelligence_time = "time") %>%
    left_join(select(stripe, id, become_member_time = "stripe_create_time", plan), by = "id") %>%
    filter(!is.na(become_member_time)) %>%
    arrange(id, desc(intelligence_time)) %>%
    mutate(minute_diff = as.integer(difftime(become_member_time, intelligence_time, units = "mins"))) %>%
    mutate(minute_diff = ifelse(intelligence_time <= "2020-10-25" & intelligence_time >= "2020-03-29", minute_diff+60, minute_diff)) %>%
    filter(minute_diff > 0 & minute_diff < 6) %>%
    group_by(id) %>%
    arrange(minute_diff) %>%
    slice(1) %>%
    ungroup()

  abandoned_utm <- upgrade_utm %>%
    filter(!distinct_id %in% ids_converted) %>%
    select(id = "distinct_id", utm_campaign, intelligence_time = "time") %>%
    left_join(select(payment_page, id = "distinct_id", payment_page_time = "time", current_url), by = "id") %>%
    arrange(id, desc(intelligence_time)) %>%
    mutate(minute_diff = as.integer(difftime(payment_page_time, intelligence_time, units = "mins"))-60) %>%
    arrange(desc(payment_page_time)) %>%
    filter(minute_diff < 6, minute_diff > 0)

  return(list(
    "upgrade_utm" = upgrade_utm,
    "payment_page" = payment_page,
    "sry_utm_upgrades" = sry_utm_upgrades,
    "abandoned_utm" = abandoned_utm
  ))

}

#' Membership conversion tracking
#' 
#' @param stripe tibble of stripe customers (see `stripe_customers`)
#' @param events_si tibble of mixpanel events from serendipity (see `mxp_get_sry_table`)
#' @param events_tpk tibble of mixpanel events from toplink (see `mxp_get_sry_table`)
#' 
#' @export
build_conversion_funnel <- function(stripe, events_si, events_tpk) {

  all_users_count <- events_si %>%  
    select(date, distinct_id, event, current_url, referring_domain) %>%
    group_by(date) %>% 
    summarize(all_users = length(unique(distinct_id))) %>% 
    ungroup() %>%
    arrange(desc(date)) %>%
    mutate(all_users = as.integer(all_users))

  all_users_count_logged_in <- events_si %>%  
    select(date, distinct_id, event, current_url, referring_domain) %>%
    filter(nchar(distinct_id) == 18) %>%
    group_by(date) %>% 
    summarize(all_users_logged_in = length(unique(distinct_id))) %>% 
    ungroup() %>%
    arrange(desc(date)) %>%
    mutate(all_users_logged_in = as.integer(all_users_logged_in))

  users_click_become_member <- events_si %>%  
    select(date, distinct_id, event, current_url, referring_domain) %>%
    filter(nchar(distinct_id) == 18 & event == "Upgrade") %>%
    group_by(date) %>% 
    summarize(users_click_become_member = length(unique(distinct_id))) %>% 
    ungroup() %>%
    arrange(desc(date)) %>%
    mutate(users_click_become_member = as.integer(users_click_become_member))

  dm_users_count <-  events_si %>%
    select(date, distinct_id, event, current_url, referring_domain) %>%
    filter(distinct_id %in% stripe$id) %>%
    group_by(date) %>% 
    summarize(digital_members_users = length(unique(distinct_id))) %>% 
    ungroup() %>%
    arrange(desc(date)) %>%
    mutate(digital_members_users = as.integer(digital_members_users))

  # from si to toplink

  member_portal_users <- events_tpk %>%
    group_by(date) %>% 
    summarize(member_portal_users = length(unique(distinct_id))) %>% 
    ungroup() %>%
    arrange(desc(date)) %>%
    mutate(member_portal_users = as.integer(member_portal_users))

  member_portal_users_logged_in <- events_tpk %>%
    filter(nchar(distinct_id) == 18) %>%
    group_by(date) %>% 
    summarize(member_portal_users_logged_in = length(unique(distinct_id))) %>% 
    ungroup() %>%
    arrange(desc(date)) %>%
    mutate(member_portal_users_logged_in = as.integer(member_portal_users_logged_in))

  payment_users <- events_tpk %>%
    filter(grepl("https://digital-members.weforum.org/intelligence/payment", current_url)) %>%
    group_by(date) %>% 
    summarize(payment_users = length(unique(distinct_id))) %>% 
    ungroup() %>%
    arrange(desc(date)) %>%
    mutate(payment_users = as.integer(payment_users))

  new_champions <- events_tpk %>%
    filter(grepl("https://digital-members.weforum.org/new-champions-membership", current_url)) %>%
    group_by(date) %>% 
    summarize(new_champions = length(unique(distinct_id))) %>% 
    ungroup() %>%
    arrange(desc(date)) %>%
    mutate(new_champions = as.integer(new_champions))

  apply_new_champions <- events_tpk %>%
    filter(grepl("https://digital-members.weforum.org/application/person-details", current_url)) %>%
    group_by(date) %>% 
    summarize(apply_new_champions = length(unique(distinct_id))) %>% 
    ungroup() %>%
    arrange(desc(date)) %>%
    mutate(apply_new_champions = as.integer(apply_new_champions))

  # from toplink to stripe

  become_member_premium <- stripe %>%
    mutate(date = as.Date(stripe_create_time)) %>%
    filter(plan == "premium") %>%
    group_by(date) %>% 
    summarize(become_member_premium = length(unique(id))) %>% 
    ungroup() %>%
    arrange(desc(date)) %>%
    mutate(become_member_premium = as.integer(become_member_premium))

  become_member_pro <- stripe %>%
    mutate(date = as.Date(stripe_create_time)) %>%
    filter(plan == "pro") %>%
    group_by(date) %>% 
    summarize(become_member_pro = length(unique(id))) %>% 
    ungroup() %>%
    arrange(desc(date)) %>%
    mutate(become_member_pro = as.integer(become_member_pro))

  become_member_nc <- stripe %>%
    mutate(date = as.Date(stripe_create_time)) %>%
    filter(plan == "Enterprise") %>%
    group_by(date) %>% 
    summarize(become_member_nc = length(unique(id))) %>% 
    ungroup() %>%
    arrange(desc(date)) %>%
    mutate(become_member_nc = as.integer(become_member_nc))

  all_users_funnel <- all_users_count %>% 
    left_join(dm_users_count, by = "date") %>%
    left_join(users_click_become_member, by = "date") %>%  
    left_join(member_portal_users, by = "date") %>% 
    left_join(payment_users, by = "date") %>% 
    left_join(new_champions, by = "date") %>% 
    left_join(become_member_premium, by = "date") %>% 
    left_join(become_member_pro, by = "date") %>% 
    left_join(become_member_nc, by = "date") %>% 
    left_join(all_users_count_logged_in, by = "date") %>% 
    left_join(apply_new_champions, by = "date") %>% 
    left_join(member_portal_users_logged_in, by = "date") %>% 
    tidyr::replace_na(
      list(
        all_users = 0,
        digital_members_users = 0,
        member_portal_users = 0,
        payment_users = 0,
        users_click_become_member = 0,
        new_champions = 0,
        become_member_premium = 0,
        become_member_pro = 0,
        become_member_nc = 0,
        apply_new_champions = 0,
        all_users_logged_in = 0,
        member_portal_users_logged_in = 0
      )
    )

  return(all_users_funnel)
}


#' SI logins from Forum domain
#' 
#' @description 
#' We track all signups that orginate from a weforum domain referral.
#' We also track all signups from weforum domains as one measure of
#' retention.
#' 
#' @param events_si tibble of mixpanel events from serendipity (see `mxp_get_sry_table`)
#' 
#' @export
mxp_track_weforum_logins <- function(events_si) {

  weforum_signups <- events_si %>%
    filter(referring_domain %in% weforum_domains & event == "Signup") %>%
    count(date) %>%
    arrange(date)

  weforum_logins <- events_si %>%
    filter(referring_domain %in% weforum_domains & nchar(distinct_id) == 18) %>%
    distinct(date, distinct_id) %>%
    arrange(date)

  return(
    list(
      "weforum_signups" = weforum_signups,
      "weforum_logins" = weforum_logins
    )
  )
}
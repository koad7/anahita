#' SI user statistics - Total Activity
#' 
#' Produce full table of key SI usage statistics for all si
#' unique users: total events, first_activity and last_activity
#' 
#' @description 
#' Calculating the table for all unique users is proven to be more efficient
#' than sending ids in the query. So we calculate these summary statistics on
#' a weekly basis.
#'
#' @param conn connection to the anahita DB
#' @param id vector of SF id of individuals (default is NULL)
#' @param time_since starting time (default is NULL)
#' @param time_to ending time (default is NULL)
#' 
#' @examples
#' \dontrun{
#' conn <- connect_anahita()
#' get_si_user_stats(conn, time_since = as.Date("2021-01-01"))
#' }
#' 
#' @return a tibble
#' 
#' @export
get_si_user_stats <- function(conn, id = NULL, time_since = NULL, time_to = NULL) {

  if (is.null(time_to)) 
    time_to <- Sys.Date()

  query <- tbl(conn, "events_clean") %>% 
    filter(time <= time_to)

  if (!is.null(id))
    query <- query %>%
      filter(distinct_id %in% id)

  if (!is.null(time_since))
    query <- query %>%
      filter(time >= time_since)

  accounts <- tbl(con_an, "sf_people_hash") %>%
    select(id, personemail, toplink_username__c, createddate, si_sub_type__c, si_sub_final_text__c, stripe_id__c, plan, dm_member_type) %>%
    collect() %>%
    mutate(toplink_username__c = ifelse(is.na(toplink_username__c), personemail, toplink_username__c)) %>%
    select(-personemail)

  domains_map <- tbl(con_an, "domain_to_org_mapping") %>%
    collect()

  query %>%
    filter(nchar(distinct_id) == 18) %>%
    mutate(Date = as.Date(time)) %>%
    group_by(distinct_id) %>%
    summarise(
      total_events = n(),
      first_activity = min(time, na.rm = TRUE),
      last_activity = max(time, na.rm = TRUE),
      total_days = n_distinct(Date),
      .groups = "drop"
    ) %>%
    collect() %>%
    mutate(total_events = as.integer(total_events)) %>%
    left_join(accounts, by = c("distinct_id" = "id")) %>%
    arrange(desc(total_events)) %>%
    mutate(domain = urltools::domain(toplink_username__c)) %>%
    #mutate(grouped_domain = assign_grouped_domain(toplink_username__c)) %>%
    mutate(domain_type = assign_domain_type(toplink_username__c)) %>%
    left_join(domains_map, by = "domain")
  
}

#' SI user statistics - Total Activity
#' 
#' Produce full table of key SI usage statistics for all si
#' unique users: total events, first_activity and last_activity
#' 
#' @description 
#' Calculating the table for all unique users is proven to be more efficient
#' than sending ids in the query. So we calculate these summary statistics on
#' a weekly basis.
#'
#' @param conn connection to the anahita DB
#' @param id vector of SF id of individuals (default is NULL)
#' @param time_since starting time (default is NULL)
#' @param time_to ending time (default is NULL)
#' 
#' @examples
#' \dontrun{
#' conn <- connect_anahita()
#' get_si_user_stats(conn, time_since = as.Date("2021-01-01"))
#' }
#' 
#' @return a tibble
#' 
#' @export
#'
get_si_user_stats <- function(conn, id = NULL, time_since = NULL, time_to = NULL) {

  if (is.null(time_to)) 
    time_to <- Sys.Date()

  query <- tbl(conn, "events_clean") %>% 
    filter(time <= time_to)

  accounts <- tbl(conn, "sf_people_hash") %>% 
    select(id, fullname__c, createddate, si_sub_final_text__c, si_sub_type__c, stripe_id__c, accountsource)

  if (!is.null(id))
    query <- query %>%
      filter(distinct_id %in% id)

  if (!is.null(time_since))
    query <- query %>%
      filter(time >= time_since)

  # build browsing summary

  query %>%
    filter(nchar(distinct_id) == 18) %>%
    mutate(Date = as.Date(time)) %>%
    left_join(accounts, by = c("distinct_id" = "id")) %>%
    group_by(distinct_id) %>%
    summarise(
      total_events = n(),
      first_activity = min(time, na.rm = TRUE),
      last_activity = max(time, na.rm = TRUE),
      total_days = n_distinct(Date),
      .groups = "drop"
    ) %>%
    ungroup() %>%
    collect() %>%
    arrange(desc(total_events)) %>%
    mutate(total_events = as.integer(total_events))

  # assign other useful metrics


  
}

#' Top SI user statistics - Location
#' 
#' Produce full table of key SI usage statistics for all si
#' unique users: top 5 briwasing cities with events.
#' 
#' @description 
#' Calculating the table for all unique users is proven to be more efficient
#' than sending ids in the query. So we calculate these summary statistics on
#' a weekly basis.
#'
#' @param conn connection to the anahita DB
#' @param nmax max numbe of cities per user (default is 5)
#' @param id vector of SF id of individuals (default is NULL)
#' @param time_since starting time (default is NULL)
#' @param time_to ending time (default is NULL)
#' 
#' @examples
#' \dontrun{
#' conn <- connect_anahita()
#' get_si_user_location(conn, time_since = as.Date("2021-01-01"))
#' get_si_user_location(conn, nmax = 10, time_since = as.Date("2021-01-01"))
#' }
#' 
#' @return a tibble
#' 
#' @export
#'
get_si_user_location <- function(conn, nmax = 5, id = NULL, time_since = NULL, time_to = NULL) {

  if (is.null(time_to)) 
    time_to <- Sys.Date()

  query <- tbl(conn, "events_clean") %>% 
    filter(time <= time_to)

  if (!is.null(id))
    query <- query %>%
      filter(distinct_id %in% id)

  if (!is.null(time_since))
    query <- query %>%
      filter(time >= time_since)

  query %>%
    filter(nchar(distinct_id) == 18) %>%
    add_count(distinct_id, name = "total_events") %>%
    group_by(distinct_id, mp_country_code, city) %>%
    summarise(
      total_events = max(total_events, na.rm = TRUE),
      city_events = n(),
      .groups = "drop"
    ) %>%
    add_count(distinct_id, name = "n_cities") %>%
    group_by(distinct_id) %>%
    slice_max(order_by = city_events, n = 5, with_ties = FALSE) %>%
    ungroup() %>% 
    arrange(distinct_id, desc(city_events)) %>%
    collect() %>%
    mutate(
      total_events = as.integer(total_events),
      city_events = as.integer(city_events)) %>%
    group_by(distinct_id) %>%
    mutate(percentage_city = city_events/total_events) %>%
    ungroup() %>%
    select(distinct_id, mp_country_code, city, n_cities, total_events, city_events, percentage_city)
  
}

#' Top SI user statistics - Top Insights
#' 
#' Produce full table of key SI usage statistics for all si
#' unique users: top 5 insights
#' 
#' @description 
#' Calculating the table for all unique users is proven to be more efficient
#' than sending ids in the query. So we calculate these summary statistics on
#' a weekly basis.
#'
#' @param conn connection to the anahita DB
#' @param id vector of SF id of individuals (default is NULL)
#' @param time_since starting time (default is NULL)
#' @param time_to ending time (default is NULL)
#' 
#' @examples
#' \dontrun{
#' conn <- connect_anahita()
#' get_si_browsing(conn, time_since = as.Date("2021-01-01"))
#' id <- "001b000003JXhbEAAT"
#' get_si_browsing(conn, id)
#' }
#' 
#' @return a tibble
#' 
#' @importFrom lubridate floor_date
#' 
#' @export
get_si_browsing <- function(conn, id = NULL, time_since = NULL, time_to = NULL) {

  browsing_events <- c(
    "Browse page", "Read Article", "A/B Experiment Play", "Follow", "Change theme",
    "Select Advanced Analytics Time Period", "Search Key Issues", "Play",
    "Watch Video", "Search", "Add Topic to Map", "A/B Experiment Win", "Follow",
    "Read Article", "Unfollow", "Play Media", "Change Language", "Upgrade",
    "Email Share", "Filter", "Filter Topic Type", "earthtime", "Feedback",
    "EarthTime", "Edit Map", "LinkedIn Share", "Copy Share", "Share", "WhatsApp Share",
    "Generate briefing", "Respond app survey", "Select Advanced Analytics Secondary Topic", 
    "Open Link", "Bookmark", "Map Image Export", "Create mapshare open invitation - API", 
    "Twitter Share", "Open Advanced Analytics", "Delete map", "Clear Map", "Create Map",
    "View Full Profile"
  )

  if (is.null(time_to)) 
    time_to <- Sys.Date()
  
  query <- tbl(conn, "events_clean") %>% 
    filter(time <= time_to)

  if (!is.null(id))
    query <- query %>%
      filter(distinct_id %in% id)

  if (!is.null(time_since))
    query <- query %>%
      filter(time > time_since)

  query %>%
    filter(nchar(distinct_id) == 18) %>%
    filter(event %in% browsing_events) %>%
    group_by(
      time, distinct_id, current_url, 
      subpage, entityId, secondaryEntityId, 
      page, language, client, event) %>%
    summarise(n_events = n(), .groups = "drop") %>%
    collect() %>%
    mutate(
      event = case_when(
        event == "Open Link" & subpage == "Publications" ~ "Read Article",
        event == "Open link" & is.na(subpage) ~ "Read Article",
        event == "Open Link" & subpage == "Data" ~ "EarthTime",
        event == "Browse page" & page == "Data detail" ~ "EarthTime",
        TRUE ~ event
    )) %>%
    rowwise() %>%
    mutate(id_url = ifelse(grepl("topics", current_url), strsplit(strsplit(current_url, "/topics/")[[1]][2], "\\?")[[1]][1], NA)) %>%
    mutate(entityId = ifelse(is.na(entityId) & !is.na(id_url), id_url, entityId)) %>%
    mutate(id_url = ifelse(is.na(id_url) & !is.na(entityId) & nchar(entityId) == 18, entityId, id_url)) %>%
    mutate(week = floor_date(as.Date(time) - 1, "weeks") + 1) %>%
    mutate(month = format(as.Date(time), "%Y-%m")) %>%
    ungroup() %>%
    select(
      time, week, month, distinct_id, current_url, id_url, 
      entityId, secondaryEntityId, page, subpage, event,
      language, client)

}

#' Top Insights
#' 
#' Produce top 5 insights areas of interest for every
#' distinct user.
#' 
#' @description 
#' Based on the results of `get_si_browsing` that aggregates all
#' browsing information into a summary table, we can quickly build
#' some key statistics around users. Here we calculate the top 5 
#' insight areas that users are interested in.
#'
#' @param conn connection to the anahita DB
#' @param nmax max numbe of insights per user (default is 5)
#' @param id vector of SF id of individuals (default is NULL)
#' @param time_since starting time (default is NULL)
#' @param time_to ending time (default is NULL)
#' 
#' @examples
#' \dontrun{
#' conn <- connect_anahita()
#' get_si_user_top_insights(conn, time_since = as.Date("2021-01-01"))
#' id <- "001b000003JXhbEAAT"
#' get_si_user_top_insights(conn, nmax = 10, id)
#' }
#' 
#' @return a tibble
#' 
#' @export
get_si_user_top_insights <- function(conn, nmax = 5, id = NULL, time_since = NULL, time_to = NULL) {

  if (is.null(time_to)) 
    time_to <- Sys.Date()
  
  query <- tbl(conn, "si_user_browsing") %>% 
    filter(time <= time_to)

  if (!is.null(id))
    query <- query %>%
      filter(distinct_id %in% id)

  if (!is.null(time_since))
    query <- query %>%
      filter(time >= time_since)

  query %>% 
    filter(time <= time_to) %>%
    filter(nchar(distinct_id) == 18) %>%
    add_count(distinct_id, name = "total_browsing_events") %>%
    group_by(distinct_id, id_url) %>%
    summarise(
      total_browsing_events = max(total_browsing_events, na.rm = TRUE),
      insights_events = n(),
      .groups = "drop"
    ) %>%
    add_count(distinct_id, name = "n_topics") %>%
    group_by(distinct_id) %>%
    slice_max(order_by = insights_events, n = 5, with_ties = FALSE) %>%
    ungroup() %>% 
    collect() %>%
    mutate(
      total_browsing_events = as.integer(total_browsing_events),
      insights_events = as.integer(insights_events)) %>%
    group_by(distinct_id) %>%
    mutate(percentage_insights = insights_events/total_browsing_events) %>%
    ungroup() %>%
    arrange(desc(total_browsing_events), distinct_id, desc(insights_events))
    
}

#' Top Key Issues
#' 
#' Produce top 5 key issues of interest for every
#' distinct user.
#' 
#' @description 
#' Based on the results of `get_si_browsing` that aggregates all
#' browsing information into a summary table, we can quickly build
#' some key statistics around users. Here we calculate the top 5 
#' key issues that users are interested in.
#'
#' @param conn connection to the anahita DB
#' @param nmax max numbe of insights per user (default is 5)
#' @param id vector of SF id of individuals (default is NULL)
#' @param time_since starting time (default is NULL)
#' @param time_to ending time (default is NULL)
#' 
#' @examples
#' \dontrun{
#' conn <- connect_anahita()
#' get_si_user_top_insights(conn, time_since = as.Date("2021-01-01"))
#' id <- "001b000003JXhbEAAT"
#' get_si_user_top_insights(conn, nmax = 10, id)
#' }
#' 
#' @return a tibble
#'
#' @export
get_si_user_top_issues <- function(conn, nmax = 5, id = NULL, time_since = NULL, time_to = NULL) {

  if (is.null(time_to)) 
    time_to <- Sys.Date()
  
  query <- tbl(conn, "si_user_browsing") %>% 
    filter(time <= time_to)

  if (!is.null(id))
    query <- query %>%
      filter(distinct_id %in% id)

  if (!is.null(time_since))
    query <- query %>%
      filter(time >= time_since)

  query %>% 
    filter(time <= time_to) %>%
    filter(nchar(distinct_id) == 18) %>%
    add_count(distinct_id, name = "total_browsing_events") %>%
    group_by(distinct_id, secondaryEntityId) %>%
    summarise(
      total_browsing_events = max(total_browsing_events, na.rm = TRUE),
      issues_events = n(),
      .groups = "drop"
    ) %>%
    add_count(distinct_id, name = "n_topics") %>%
    group_by(distinct_id) %>%
    slice_max(order_by = issues_events, n = 5, with_ties = FALSE) %>%
    ungroup() %>% 
    collect() %>%
    mutate(
      total_browsing_events = as.integer(total_browsing_events),
      issues_events = as.integer(issues_events)) %>%
    group_by(distinct_id) %>%
    mutate(percentage_insights = issues_events/total_browsing_events) %>%
    ungroup() %>%
    arrange(desc(total_browsing_events), distinct_id, desc(issues_events))
    
}

#' Assign category
#' 
#' Assign category to SI users.
#' 
#' @description 
#' For business reporting purposes, we want to assign every individual
#' SI user to high level category based on the SF account type. 
#'
#' @param record_type_id vector of account recort type id
#' 
#' @examples
#' \dontrun{
#' si_usage_assign_dm_record_type(c("012b00000000ARJAA2", "012b00000000ARJAA2"))
#' }
#' 
#' @return a character string
#'
#' @export
si_usage_assign_dm_record_type <- function(record_type_id){ 

  record_types <- sapply(
    record_type_id,
    function(x) {

      if (x == "012b00000000ARJAA2") {
        type <- "Constituent"
      } else if (x == "012b00000000ARMAA2") {
        type <- "Staff"
      } else if (x == "012b0000000cgBxAAI") {
        type <- "Lite"
      } else if (x == "012b00000000O7YAAU") {
        type <- "Other"
      } else {
        type <-"Other"
      }
      
      return(type)
    }
  )

  names(record_types) <- NULL
  return(record_types)

}

#' Assign category
#' 
#' Assign category to SI users.
#' 
#' @description 
#' For business reporting purposes, we want to assign every individual
#' SI user a category reflecting its digital membership status such as
#' being a paying membership or a whitelisted digital member.
#'
#' @param record_type_id account recort type id
#' @param si_sub_type digital membership account categorization
#' @param si_membership membership level (Public, Premium, Pro, Platinum)
#' @param stripe_status stripe status plan 
#' 
#' @examples
#' \dontrun{
#' record_type_id <- "012b00000000ARJAA2"
#' si_sub_type <- "TopLink"
#' si_membership <- "Pro"
#' stripe_status <- "active"
#' si_usage_assign_dm_member_type(record_type, si_sub_type, si_membership, stripe_status)
#' }
#' 
#' @return a character string
#'
#' @export
si_usage_assign_dm_member_type <- function(record_type_id, si_sub_type, si_membership, stripe_status){ 

  dt <- tibble(bind_cols(
    "record_type_id" = record_type_id,
    "si_sub_type" = si_sub_type,
    "si_membership" = si_membership, 
    "stripe_status" = stripe_status))

  apply(
    dt,
    1,
    function(x) {
      
      type <- NA

      if (x[1] == "012b00000000ARJAA2") {
      
        # whitelisted members
        if (x[2] == "Organization" & x[3] == "Premium")
          type <- "Digital Member - Institutional"
        
        # includes both paying and overwrites, paying overwritten later
        if (x[2] == "Personal")
          type <- "Digital Member - Other"
        
        # constituents
        if (x[2] == "TopLink")
          type <- "Constituent"

        # whatever the subtype, if stripe status is active, trialing or past-due, these are paying members
        if (!is.na(x[4]) & !x[4] %in% c("canceled", "incomplete"))
          type <- "Digital Member - Paying"
    
      } else if (x[1] == "012b00000000ARMAA2") {
          type <- "Staff"
      } else if (x[1] == "012b0000000cgBxAAI" | x[3] == "Public") {
          type <- "Public User"
      } else if (x[1] == "012b00000000O7YAAU") {
          type <- "Other"
      }

      return(type)
  }  
  )
}

#' Extract domain
#' 
#' Extract email domain
#' 
#' @description 
#' The email domain is used to derive information on the
#' user's stakeholder group (professional versus academic e.g.)
#' and affiliation to a know company (based on SF or CB domain
#' mapping)
#'
#' @param emails vector of emails
#' 
#' @examples
#' \dontrun{
#' emails <- c("ss@dd.com", "test@ddd.fr", "dsdsd@hrd.de")
#' si_usage_extract_domain(emails)
#' }
#' 
#' @return a vector
#' 
#' @importFrom urltools domain
#'
#' @export
si_usage_extract_domain <- function(emails) {
  sapply(
    emails,
    USE.NAMES = FALSE,
    function(x) {
      urltools::domain(x)
    })
}

#' Extract extension
#' 
#' Extract domnain extension
#' 
#' @description 
#' A unique company domain can be declined with several
#' extension identifying for example the country.
#'
#' @param emails vector of emails or domains
#' 
#' @examples
#' \dontrun{
#' emails <- c("ss@dd.com", "ddd.fr", "dsdsd@hrd.de")
#' si_usage_extract_domain(emails)
#' }
#' 
#' @return a vector
#'
#' @export
si_usage_extract_extension <- function(emails) {

  sapply(
    emails,
    USE.NAMES = FALSE,
    function(x) {
      domain <- x
      if (grepl("@", domain))
        domain <- si_usage_extract_domain(domain)
      gsub(".*\\.(.*)", "\\1", domain)
    })
}

#' Email type
#' 
#' User email type
#' 
#' @description 
#' We try to infer the user's stakeholder group by looking at the
#' email domain and or the extension and see if it matches any of the
#' reference lists we have.
#'
#' @param conn connection to anahita DB
#' @param emails vector of emails or domains
#' 
#' @examples
#' \dontrun{
#' conn <- connect_anahita()
#' emails <- c("ss@dd.com", "ddd.fr", "dsdsd@hrd.de")
#' si_usage_assign_email_type(conn, emails)
#' }
#' 
#' @return a vector
#'
#' @export
si_usage_assign_email_type <- function(conn, emails) {

  generic_domains <- tbl(conn, "generic_domains") %>% 
    collect()

  generic_domains_grouped <- tbl(conn, "generic_domains_grouped") %>% 
    collect()

  sapply(
    emails,
    USE.NAMES = FALSE,
    function(x) {
      domain <- x
      if (grepl("@", domain))
        domain <- si_usage_extract_domain(domain)

      if (domain %in% generic_domains$Generic_Domains | domain %in% generic_domains_grouped$Generic_Grouped_Domain) {
        email_type <- "Generic"
      } else if (is.na(domain) | domain == "" ) {
        email_type <- "Undefined"
      } else {
        email_type <- "Professional"
      }
    
      return(email_type)
    })

}

#' Grouped domain
#' 
#' User grouped domain affiliation
#' 
#' @description 
#' We assign to each emnail an domain group coinciding with the way
#' we build grouped domains based on SF TLO versus subsidary
#' organisations.
#'
#' @param conn connection to anahita DB
#' @param emails vector of emails or domains
#' 
#' @examples
#' \dontrun{
#' conn <- connect_anahita()
#' emails <- c("ss@dd.com", "ddd.fr", "dsdsd@hrd.de")
#' si_usage_assign_grouped_domain(conn, emails)
#' }
#' 
#' @return a vector
#'
#' @importFrom stringr str_split
#' 
#' @export
si_usage_assign_grouped_domain <- function(conn, emails) {

  recognized_domain_extensions <- tbl(conn, "recognized_extensions") %>% 
    collect()
  
  sapply(
    emails,
    USE.NAMES = FALSE,
    function(x) {

      domain <- x

      if (grepl("@", domain))
        domain <- si_usage_extract_domain(domain)

      cleaned_domain <- tibble("splits" = str_split(domain, pattern = "\\.")[[1]]) %>%
        filter(!splits %in% recognized_domain_extensions$extension)
      
      group <- paste0(cleaned_domain$splits, collapse = "")

      if (group == "" | group == "NA" | is.na(group))
        group <- domain

      return(group)

    }
  )

}

.find_sf_orgs <- function(conn1, conn2, grouped_domain) {

  toplist <- tbl(conn1, in_schema("ods", "sfdc_account_v")) %>%
    filter(grepl("TopList Business", account_value_network_roles__c)) %>%
    select(id, name) %>%
    collect() %>%
    mutate(is_top_list = TRUE)

  domain_to_tlo_mapping <- tbl(conn2, "DTLOmap") %>% 
    collect() %>%
    left_join(toplist, by = c("tlo" = "name")) %>%
    group_by(tlo) %>%
    arrange(id) %>%
    slice(1) %>%
    ungroup() %>%
    mutate(is_top_list = ifelse(is.na(is_top_list), FALSE, is_top_list))

  grouped_domain <- tibble("grouped_domain" = grouped_domain) %>%
    left_join(domain_to_tlo_mapping, by = "grouped_domain")
  
  return(grouped_domain$tlo)

}

.find_cb_orgs <- function(conn2, domain) {

  tbl_cb_id <- tbl(conn2, "cb_organizations") %>%
    filter(!is.na(domain)) %>%
    add_count(domain) %>%
    filter(n == 1) %>%
    select(-n) %>%
    select(uuid, cb_name = name, domain) %>%
    collect() 

  domain <- tibble("domain" = domain) %>%
    left_join(tbl_cb_id, by = "domain")

  return(domain$cb_name)

}

#' Assign Organisation
#' 
#' Assign organisation to SI users.
#' 
#' @description 
#' For business reporting purposes, we want to assign every individual
#' SI user an guess on their affiliated Top Level Organisation in SF, if
#' possible based on the email domain or SF account.
#'
#' @param conn1 connection to the DW
#' @param conn2 connection to anahita
#' @param dt accounts data frame
#' 
#' @examples
#' \dontrun{
#' conn1 <- connect_dw()
#' conn2 <- connect_anahita()
#' accounts <- tbl(conn1, dbplyr::in_schema("ods","sfdc_account_v")) %>% 
#'  filter(si_sub_final_text__c == "Pro") %>%
#'  collect()
#' assigned_tlos <- si_usage_assign_tlo(conn2, accounts)
#' }
#' 
#' @return a tibble
#'
#' @export
si_usage_assign_org <- function(conn1, conn2, dt) {

  # Salesforce association

  dt %>%
    mutate(org_sf = .find_sf_orgs(conn1, conn2, dt$grouped_domain)) %>%
    mutate(org_cb = .find_cb_orgs(conn2, dt$domain)) %>%
    mutate(org_final = ifelse(
      !is.na(top_level_organization__pc),
      top_level_organization__pc,
      ifelse(
        !is.na(org_sf),
        org_sf,
        org_cb
      )
    )) %>%
    mutate(org_final = ifelse(
      email_type == "Generic",
      NA,
      org_final
    )) %>%
    mutate(org_cb = ifelse(
      email_type == "Generic",
      NA,
      org_cb
    )) %>%
    mutate(org_source = ifelse(
      !is.na(top_level_organization__pc) | !is.na(org_sf),
      "sf",
      ifelse(
        !is.na(org_cb),
        "cb",
        NA
      )
    ))

}
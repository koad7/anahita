#' Update events table for DMS
#' 
#' @description 
#' Certain events being value proposition for DMS, we track
#' actively their registration and participation.
#' 
#' @param dev deployment status read from config (dev will not right into DBs)
#' 
#' @examples 
#' \dontrun{
#' update_events_tables()
#' }
#' 
#' @return list of tibbles
#' 
#' @export
update_events_tables <- function(dev = config_get(deploy_status)) {

  cat("\n\n########### Update events table ###########\n")
  cat("---> Deployment status = ", dev, "\n\n")

  con_dw <- connect_dw()
  con_an <- connect_anahita()
  #con_dm <- connect_dm()

  on.exit({
    cat("Closing connections")
    DBI::dbDisconnect(con_dw)
    DBI::dbDisconnect(con_an)
    #DBI::dbDisconnect(con_dm)
    add = TRUE
    })

  cat("0. Preparing data\n")

  # retrieve groups of interest

  stripe <- tbl(con_an, "stripe_customers") %>%
    select(stripe_id = "customer_ids", id, stripe_create, stripe_status = "status", stripe_plan = "plan", stripe_to_cancel = "to_cancel", stripe_canceled_date = "canceled_date") %>%
    collect()

  all_dms <- tbl(con_an, "dms_people") %>%
    select(id, name, personemail = "toplink_username__c", createddate, si_sub_final_text__c, si_sub_type__c, stripe_id__c) %>%
    filter(si_sub_final_text__c == "Premium" | si_sub_final_text__c == "Pro") %>%
    filter(si_sub_type__c == "Organization" | si_sub_type__c == "Personal") %>%
    collect()
    
  ids <- unique(c(stripe$id, all_dms$id))

  cat("0.1 Running initializtion for", length(ids), "DM salesforce ids\n")

  # domain intelligence to match companies

  whitelist <- tbl(con_an, "sf_whitelisting") %>%
    select(domain = "email_domain__c", company_sfid = "account__c", status__c, org_name = "name") %>%
    collect()

  # to get additional event and account information

  account_info <- tbl(con_dw, in_schema("ods", "sfdc_account_v")) %>%
    filter(id %in% ids) %>%
    select(id, name, personemail = "toplink_username__c", createddate, si_sub_final_text__c, si_sub_type__c, stripe_id__c) %>%
    collect() %>%
    left_join(stripe, by = "id")

  cat("1. Webinars\n")

  emails <- account_info$personemail

  webinars <- events_webinar_attendance(con_dw, emails) %>%
    left_join(select(account_info, personemail, salesforce_id = "id"), by = c("user_email" = "personemail"))

  cat("1.1 Found", nrow(webinars), "webinar entries\n")

  cat("2. Other events\n")

  max_date <- tbl(con_an, "dms_events") %>%
    select(session_start_date_utc) %>% 
    filter(session_start_date_utc == max(session_start_date_utc, na.rm = TRUE)) %>% 
    distinct(session_start_date_utc) %>%
    collect() %>%
    pull(session_start_date_utc)

  list_sessions <- tbl(con_an, "dms_events") %>%
    filter(session_start_date_utc == max_date) %>%
    distinct(session_sfdc_id) %>%
    collect() %>%
    pull(session_sfdc_id)

  dms_events <- events_attendance(con_dw, ids, max_date, list_sessions)

  cat("2.1 Match accounts and domains\n")

  dms_events <- dms_events %>%
    left_join(account_info, by = c("account_sfdc_id" = "id")) %>%
    mutate(domain = urltools::domain(personemail)) %>%
    left_join(whitelist, by = "domain") %>%
    mutate(organization_sfdc_id = ifelse(organization_sfdc_id == "None" & !is.na(company_sfid), company_sfid, organization_sfdc_id)) %>%
    mutate(organization_name = ifelse(organization_name == "None" & !is.na(org_name), org_name, organization_name)) %>%
    mutate(whitelisted = ifelse(is.na(company_sfid), FALSE, TRUE)) %>%
    select(c(-company_sfid, -org_name, name))

  if (nrow(dms_events) > 0) {

    cat("2.2 Match companies\n")

    comp_ids <- dms_events %>%
      filter(organization_sfdc_id != "None" & !is.na(organization_sfdc_id)) %>%
      distinct(organization_sfdc_id) %>%
      pull(organization_sfdc_id)

    contracts <- tbl(con_dw, in_schema("ods", "sfdc_servicecontract_v")) %>%
      filter(contract_status__c == "Activated/Running" | contract_status__c == "Activated/Upcoming") %>%
      select(accountid, engagement_level__c, membership_type__c)

    industries <- tbl(con_dw, in_schema("ods", "sfdc_industry_v")) %>%
      select(id, industry = "name")

    comp <- tbl(con_dw, in_schema("ods", "sfdc_account_v")) %>%
      filter(id %in% comp_ids) %>%
      select(id, billingcountry, main_industry_sector__c, region__c) %>%
      left_join(industries, by = c("main_industry_sector__c" = "id")) %>%
      left_join(contracts, by = c("id" = "accountid")) %>%
      select(id, billingcountry, region__c, industry, engagement_level__c, membership_type__c) %>%
      collect()

    cat("2.3 Match domain type \n")

    dms_events <- dms_events %>%
      left_join(comp, by = c("organization_sfdc_id" = "id")) %>%
      mutate(email_type = assign_domain_type(con_an, personemail))

  }

  cat("2.4 Found", nrow(dms_events), "events entries\n")

  if (dev == "prod") {
    cat("---> Pushing table to server\n") 
    if (nrow(dms_events) > 0) 
      DBI::dbWriteTable(con_an, "dms_events", dms_events, append = TRUE, overwrite = FALSE)

    DBI::dbWriteTable(con_an, "dms_event_webinars", webinars, overwrite = TRUE)
  }

  cat("\n\n########### Update procedure completed ###########\n")

  return(list(
    "webinars" = webinars,
    "events" = dms_events
  ))

}

#' Initialize events table for DMS
#' 
#' @description 
#' Certain events being value proposition for DMS, we track
#' actively their registration and participation.
#' 
#' @param dev deployment status read from config (dev will not right into DBs)
#' 
#' @examples 
#' \dontrun{
#' initialize_events_tables()
#' }
#' 
#' @return list of tibbles
#' 
#' @export
initialize_events_tables <- function(dev = config_get(deploy_status)) {

  cat("\n\n########### Initialize events table ###########\n")
  cat("---> Deployment status = ", dev, "\n\n")

  con_dw <- connect_dw()
  con_an <- connect_anahita()
  #con_dm <- connect_dm()

  on.exit({
    cat("Closing connections")
    DBI::dbDisconnect(con_dw)
    DBI::dbDisconnect(con_an)
    #DBI::dbDisconnect(con_dm)
    add = TRUE
    })

  cat("0. Preparing data\n")

  # retrieve groups of interest

  stripe <- tbl(con_an, "stripe_customers") %>%
    select(stripe_id = "customer_ids", id, stripe_create, stripe_status = "status", stripe_plan = "plan", stripe_to_cancel = "to_cancel", stripe_canceled_date = "canceled_date") %>%
    collect()

  all_dms <- tbl(con_an, "dms_people") %>%
    select(id, name, personemail = "toplink_username__c", createddate, si_sub_final_text__c, si_sub_type__c, stripe_id__c) %>%
    filter(si_sub_final_text__c == "Premium" | si_sub_final_text__c == "Pro") %>%
    filter(si_sub_type__c == "Organization" | si_sub_type__c == "Personal") %>%
    collect()
    
  ids <- unique(c(stripe$id, all_dms$id))

  cat("0.1 Running initializtion for", length(ids), "DM salesforce ids\n")

  # domain intelligence to match companies

  whitelist <- tbl(con_an, "sf_whitelisting") %>%
    select(domain = "email_domain__c", company_sfid = "account__c", status__c, org_name = "name") %>%
    collect()

  # to get additional event and account information

  account_info <- tbl(con_dw, in_schema("ods", "sfdc_account_v")) %>%
    filter(id %in% ids) %>%
    select(id, name, personemail = "toplink_username__c", createddate, si_sub_final_text__c, si_sub_type__c, stripe_id__c) %>%
    collect() %>%
    left_join(stripe, by = "id")

  cat("1. Webinars\n")

  emails <- account_info$personemail

  webinars <- events_webinar_attendance(con_dw, emails) %>%
    left_join(select(account_info, personemail, salesforce_id = "id"), by = c("user_email" = "personemail"))

  cat("1.1 Found", nrow(webinars), "webinar entries\n")

  cat("2. Other events\n")

  dms_events <- events_attendance(con_dw, ids, max_date = NULL, sessions = NULL)

  cat("2.1 Match accounts and domains\n")

  dms_events <- dms_events %>%
    left_join(account_info, by = c("account_sfdc_id" = "id")) %>%
    mutate(domain = urltools::domain(personemail)) %>%
    left_join(whitelist, by = "domain") %>%
    mutate(organization_sfdc_id = ifelse(organization_sfdc_id == "None" & !is.na(company_sfid), company_sfid, organization_sfdc_id)) %>%
    mutate(organization_name = ifelse(organization_name == "None" & !is.na(org_name), org_name, organization_name)) %>%
    mutate(whitelisted = ifelse(is.na(company_sfid), FALSE, TRUE)) %>%
    select(c(-company_sfid, -org_name, name))

  cat("2.2 Match companies\n")

  comp_ids <- dms_events %>%
    filter(organization_sfdc_id != "None" & !is.na(organization_sfdc_id)) %>%
    distinct(organization_sfdc_id) %>%
    pull(organization_sfdc_id)

  contracts <- tbl(con_dw, in_schema("ods", "sfdc_servicecontract_v")) %>%
    filter(contract_status__c == "Activated/Running" | contract_status__c == "Activated/Upcoming") %>%
    select(accountid, engagement_level__c, membership_type__c)

  industries <- tbl(con_dw, in_schema("ods", "sfdc_industry_v")) %>%
    select(id, industry = "name")

  comp <- tbl(con_dw, in_schema("ods", "sfdc_account_v")) %>%
    filter(id %in% comp_ids) %>%
    select(id, billingcountry, main_industry_sector__c, region__c) %>%
    left_join(industries, by = c("main_industry_sector__c" = "id")) %>%
    left_join(contracts, by = c("id" = "accountid")) %>%
    select(id, billingcountry, region__c, industry, engagement_level__c, membership_type__c) %>%
    collect()

  cat("2.3 Match domain type \n")
  
  dms_events <- dms_events %>%
    left_join(comp, by = c("organization_sfdc_id" = "id")) %>%
    mutate(email_type = assign_domain_type(con_an, personemail))

  cat("2.4 Found", nrow(dms_events), "events entries\n")

  if (dev == "prod") {
    cat("---> Pushing table to server\n") 
    DBI::dbWriteTable(con_an, "dms_events", dms_events, overwrite = TRUE)
    DBI::dbWriteTable(con_an, "dms_event_webinars", webinars, overwrite = TRUE)
  }

  return(list(
    "webinars" = webinars,
    "events" = dms_events
  ))

  cat("\n\n########### Initialization procedure completed ###########\n")
}
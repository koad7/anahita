#' Retrieve SF company
#' 
#' @description 
#' In the current anahita parading, Salesforce TLOs are the reference
#' for all matching procedures to other source of data like CrunchBase. We
#' however also pull inactive company accounts to track historical records
#' when looking at potential company engagement.
#' 
#' @param conn connection to the data warehouse
#' 
#' @examples
#' \dontrun{
#' conn <- connect_dw()
#' get_sf_orgs(conn)
#' }
#' 
#' @return a tibble
#' 
#' @export
get_sf_orgs <- function(conn){
  
  rename_sf_table <- function(.){
    c("sfid", "name", "legal_name", "url", "stock_ticker", 
    "status", "industry", "org_type", "org_subtype", 
    "country", "region", "pem", "description", "city",
    "street", "created_date", "industry_name")
  }
  
  var_sel <- c(
    "id", "name", "legalname__c", "website", 
    "tickersymbol", "status__c","main_industry_sector__c", 
    "organizationtype__c", "organizationsubtype__c", 
    "organization_country_name__c","region__c", 
    "primary_engagement_manager_name__c", "organizationprofile__c", 
    "city1__c","street__c", "createddate")

  industries <- tbl(conn, in_schema("ods", "sfdc_industry_v")) %>%
    filter(status__c == "Active") %>% 
    select(id, name)
  
  tbl(conn, in_schema("ods", "sfdc_account_v")) %>%
    filter(recordtypeid == "012b00000000O7RAAU") %>%
    select(all_of(var_sel)) %>%
    left_join(industries, by = c("main_industry_sector__c" = "id")) %>%
    collect() %>%
    rename_all(rename_sf_table) %>%
    mutate(industry = industry_name) %>%
    select(-industry_name) %>%
    mutate(url = tolower(url)) %>%
    mutate(url_clean = .url_to_domain(url)) %>%
    as_tibble()
  
}

#' Retrieve SF topics
#' 
#' @description 
#' Retrieve list of all insight areas
#' 
#' @param conn connection to the data warehouse
#' 
#' @examples
#' \dontrun{
#' conn <- connect_dw()
#' get_sf_topics(conn)
#' }
#' 
#' @return a tibble
#' 
#' @export
get_sf_topics <- function(conn){
  
  tbl(conn, dbplyr::in_schema("ods","sfdc_topic_v")) %>% 
    collect() %>% 
    as_tibble() %>% 
    mutate(recordtype = ifelse(recordtypeid == "012b0000000M4SAAA0", "Insight Area", "Key Issue"))

}

#' Retrieve SF people
#' 
#' @description 
#' Retrieve of salesforce people accounts and calculate
#' hash256 value based on SF id. This is used to analyse 
#' encrypted SF ids sent by Serendipity.
#' 
#' @param conn1 connection to the data warehouse
#' @param conn2 connection to anahita
#' 
#' @examples
#' \dontrun{
#' conn1 <- connect_dw()
#' conn2 <- connect_anahita()
#' get_sf_people(conn1, conn2)
#' }
#' 
#' @return a tibble
#' 
#' @export
get_sf_people <- function(conn1, conn2) {
  
  cat("Retrieving stripe customer data...\n")

  stripe <- anahita::stripe_customers(conn2) %>%
    select(id, stripe_id = "customer_ids", stripe_create, stripe_status = "status", stripe_plan = "plan")

  # retrieve account data and add important stripe information

  cat("Retrieving accounts from SF...\n")

  accounts <- tbl(conn1, dbplyr::in_schema("ods","sfdc_account_v")) %>%
    filter(ispersonaccount == TRUE & status__c == "Active") %>%
    select(all_of(var_sel_people)) %>%
    collect() %>%
    left_join(stripe, by = "id") %>%
    rowwise() %>%
    mutate(hash256 = digest::digest(id, algo = "sha256", serialize = FALSE)) %>%
    ungroup()

  # assign useful DM categorization

  cat("Assigning Digital Membership identifiers...\n")

  accounts <- accounts %>%
    mutate(dm_record_type = si_usage_assign_dm_record_type(recordtypeid)) %>%
    mutate(dm_member_type = si_usage_assign_dm_member_type(recordtypeid, si_sub_type__c, si_sub_final_text__c, stripe_status))

  # add domain analysis

  cat("Domain analysis...\n")

  accounts <- accounts %>%
    mutate(personemail = ifelse(is.na(personemail), toplink_username__c, personemail)) %>%
    mutate(domain = si_usage_extract_domain(personemail)) %>%
    mutate(grouped_domain = si_usage_assign_grouped_domain(conn2, domain)) %>%
    mutate(extension = si_usage_extract_extension(domain)) %>%
    mutate(email_type = si_usage_assign_email_type(conn2, domain))

  # assign tlo

  cat("Organisation Assignment...\n")

  si_usage_assign_org(conn1, conn2, accounts)

}


#' User ids and email list
#' 
#' @description 
#' Sometimes, mixpanel stores emails instead of SF ids. We need
#' to replace emails by ids using a reference list of email to id
#' mappings. We need to note more than 1 email can be associated to
#' an single id since we have the person emnail and the toplink email.
#' In principle, the toplink email is the right one but we never now.
#'
#' @param conn1 connection to the anahita DB
#'
#' @examples
#' \dontrun{
#' conn1 <- connect_anahita()
#' df <- mxp_build_user_ids_emails(conn1)
#' }
#' 
#' @return a tibble
#' 
#' @export
sf_build_user_emails_to_id <- function(conn1) {

  users <- tbl(conn1, "sf_people_hash") %>%
    filter(!(is.na(toplink_username__c) & is.na(personemail))) %>%
    select(id, personemail, toplink_username__c) %>%
    collect()

  users_toplink <- users %>%
    filter(!is.na(toplink_username__c)) %>%
    select(id, toplink_username__c)

  users %>%
    filter(!is.na(personemail)) %>%
    filter(!personemail %in% users_toplink$toplink_username__c) %>%
    select(id, toplink_username__c = "personemail") %>%
    distinct(toplink_username__c, .keep_all = TRUE) %>%
    bind_rows(users_toplink) %>%
    mutate(toplink_username__c = tolower(toplink_username__c))

}
#' Get whitelisted domains
#' 
#' Get list all whitelisted domains in Salesforce
#' or domains whitelisted for a selected organisation.
#' 
#' @param conn connection to the data warehouse
#' @param org_ids list of organisation ids
#' 
#' @examples 
#' \dontrun{
#' conn <- connect_dw()
#' get_whitelisted_domains(conn)
#' get_whitelisted_domains(conn, "001b0000005gxmlAAA")
#' }
#' 
#' @return a tibble
#' 
#' @export
get_whitelisted_domains <- function(conn, org_ids = NULL) {

  list_accounts <- tbl(conn, dbplyr::in_schema("ods","sfdc_account_v")) %>%
    select(id, name)

  if (!is.null(org_ids)) {
    tbl(conn, dbplyr::in_schema("ods", "sfdc_whitelist_domain_v")) %>% 
      filter(account__c %in% org_ids) %>%
      select(account__c, createddate, email_domain__c, status__c) %>%
      left_join(list_accounts, by = c("account__c" = "id")) %>%
      collect()
  } else {
    tbl(conn, dbplyr::in_schema("ods", "sfdc_whitelist_domain_v")) %>% 
      select(account__c, createddate, email_domain__c, status__c) %>%
      left_join(list_accounts, by = c("account__c" = "id")) %>%
      collect()
  }
  
}

#' Match tlo
#' 
#' Match the tlo based on the whitelisting information
#' available.
#' 
#' @param conn connection to the data warehouse
#' @param people_tbl tibble of people accounts (see `get_ptms` or `get_dms`)
#' 
#' @examples 
#' \dontrun{
#' conn <- connect_dw()
#' dms <- get_dms(conn)
#' match_tlo(conn, dms)
#' }
#' 
#' @return a tibble
#' 
#' @export
match_tlo <- function(conn, people_tbl) {

  org_ids <- unique(people_tbl[[2]]$account__c)

  org_details <- tbl(conn, dbplyr::in_schema("ods","sfdc_account_v")) %>%
    filter(recordtypeid == "012b00000000O7RAAU" & id %in% org_ids) %>%
    select(top_level_organization__c = "id", tlo_name = "name") %>%
    collect()

  people_tbl[[1]] %>%
    left_join(select(people_tbl[[2]], top_level_organization__c = "account__c", email_domain__c), by = c("domain" = "email_domain__c")) %>%
    left_join(org_details, by = "top_level_organization__c") %>%
    rowwise() %>%
    mutate(top_level_organization__pc = ifelse(is.na(top_level_organization__pc), tlo_name, top_level_organization__pc)) %>%
    ungroup() %>%
    select(-tlo_name)

}
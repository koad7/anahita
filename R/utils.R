#' @noRd
#' @keywords internal
.all_active_ids <- function(dt) {
  tlo_ids <- dt %>%
    filter(contract_status == "Activated/Running" & !is_tlo) %>%
    select(tlo_id) %>%
    pull()
  org_ids <- dt %>%
    filter(contract_status == "Activated/Running" & is_tlo) %>%
    select(org_id) %>%
    pull()
  c(tlo_ids, org_ids)
}

#' @noRd
#' @keywords internal
.all_active_names <- function(dt) {
  tlo_names <- dt %>%
    filter(contract_status == "Activated/Running" & !is_tlo) %>%
    select(tlo_name) %>%
    pull()
  org_names <- dt %>%
    filter(contract_status == "Activated/Running" & is_tlo) %>%
    select(org_name) %>%
    pull()
  c(tlo_names, org_names)
}

#' @noRd
#' @keywords internal
.last_updated <- function() {
  tibble::tibble(last_updated = lubridate::with_tz(Sys.time(), "UTC"))
}

#' Clean URL
#' 
#' @param myurl vector of urls
#' 
#' @export
.url_to_domain <- function(myurl){
  
  sapply(
    myurl,
    function(x) {
      myurl <- tail(strsplit(x, "//")[[1]], n=1)
      myurl <- tail(strsplit(myurl, "www[.]")[[1]], n=1)
      strsplit(myurl, "/")[[1]][1]
    }
  )
}


#' Get Organisations
#' 
#' Get list of organisations based on contract characteristics. 
#' 
#' @description 
#' For safety, one is asked to provide both the engagement level and the the
#' contract membership type. These can be reviewed with the method `get_contract_membership_types`
#' 
#' @param conn connection to the data warehouse
#' @param engagement_levels vector that defines the contract engagement type
#' @param membership_types vector that defines the contract engagement type
#' 
#' @examples
#' \dontrun{
#' conn <- connect_dw()
#' get_tlo_contracts(conn, "New Champion")
#' }
#' 
#' @return tibble
#' 
#' @import dplyr
#' @import crayon
#' @importFrom jsonlite fromJSON 
#' @importFrom dbplyr in_schema
#' @importFrom RPostgres Postgres
#' @importFrom DBI dbConnect dbDisconnect dbWriteTable dbGetQuery
#' @importFrom lubridate add_with_rollback
#' @importFrom stringr str_sub
#' @importFrom tidyr pivot_longer separate_rows
#' @importFrom tibble tibble as_tibble
#' @importFrom cli cli_h1 cli_alert_danger cli_process_start cli_process_done cli_alert_info
#' @importFrom cli cli_alert_success cli_h2 cli_li cli_process_failed cli_ul
#' @importFrom readr read_csv cols
#' @importFrom utils download.file tail untar
#' 
#' @export
get_tlo_contracts <- function(conn, engagement_levels, membership_types) {

  set_col_names <- function(.){
    c("org_id", "tlo_id", "asset_end_date", "asset_start_date", 
    "contract_status", "org_name", "org_country", "org_region", 
    "org_pem_1", "org_pem_2", "org_industry", "org_profile", 
    "org_website", "org_profile_completeness", "tlo_name", "tlo_country",  
    "tlo_region", "tlo_pem_1", "tlo_pem_2", "tlo_industry", "tlo_profile", 
    "tlo_website", "tlo_profile_completeness"
    )}

  sel_vars <- c(
    "accountid", "top_level_organization__c", "asset_enddate__c", "asset_startdate__c", 
    "contract_status__c", "name.x", "organization_country_name__c.x", "region__c.x", 
    "primary_engagement_manager_name__c.x", "secondary_engagement_manager__c.x", "name.x.x",
    "organizationprofile__c.x", "website.x", "orgprofile_completeness__c.x", 
    "name.y", "organization_country_name__c.y", "region__c.y", "primary_engagement_manager_name__c.y",
    "secondary_engagement_manager__c.y", "name.y.y", "organizationprofile__c.y", 
    "website.y", "orgprofile_completeness__c.y"
  )

  sel_from_accounts <- c(
    "id", "name", "organization_country_name__c", 
    "region__c", "primary_engagement_manager_name__c", 
    "secondary_engagement_manager__c", "organizationprofile__c",
    "main_industry_sector__c", "website", "orgprofile_completeness__c"
  )

  sel_from_contracts <- c(
    "accountid","top_level_organization__c", 
    "asset_enddate__c", "asset_startdate__c", 
    "contract_status__c")

  org_details <- tbl(conn, in_schema("ods","sfdc_account_v")) %>%
    select(all_of(sel_from_accounts))

  industry_details <- tbl(conn, in_schema("ods", "sfdc_industry_v")) %>%
    select(id, name)

  tbl(conn, in_schema("ods","sfdc_servicecontract_v")) %>%
    filter(recordtypeid == "012b00000009cmTAAQ") %>%
    filter(engagement_level__c %in% engagement_levels) %>%
    filter(membership_type__c %in% membership_types) %>%
    select(all_of(sel_from_contracts)) %>%
    left_join(org_details, by = c("accountid" = "id")) %>%
    left_join(org_details, by = c("top_level_organization__c" = "id")) %>%
    left_join(industry_details, by = c("main_industry_sector__c.x" = "id")) %>%
    left_join(industry_details, by = c("main_industry_sector__c.y" = "id")) %>%
    select(-c(main_industry_sector__c.x, main_industry_sector__c.y)) %>%
    select(all_of(sel_vars)) %>%
    collect() %>%
    rowwise() %>%
    mutate(orgprofile_completeness__c.x = as.integer(str_sub(orgprofile_completeness__c.x, -3, -2))) %>%
    mutate(orgprofile_completeness__c.y = as.integer(str_sub(orgprofile_completeness__c.y, -3, -2))) %>%
    ungroup() %>%
    as_tibble() %>%
    rename_all('set_col_names')

}

#' Get Organisations
#' 
#' Get list of organisations based on membership affiliation
#' 
#' @description 
#' Contract based communities are automatically synced with contract information.
#' However, some communities are not contract based and should be accessed by querying
#' directly the community.
#' 
#' @param conn connection to the data warehouse
#' @param community_ids vector of community ids
#' 
#' @examples
#' \dontrun{
#' conn <- connect_dw()
#' get_tlo_communities(conn, "New Champion")
#' }
#' 
#' @return tibble
#' 
#' @export
get_tlo_communities <- function(conn, community_ids) {

  set_col_names <- function(.){
    c("org_id", "tlo_id", "asset_end_date", "asset_start_date", 
    "contract_status", "org_name", "org_country", "org_region", 
    "org_pem_1", "org_pem_2", "org_industry", "org_profile", 
    "org_website", "org_profile_completeness", "tlo_name", "tlo_country",  
    "tlo_region", "tlo_pem_1", "tlo_pem_2", "tlo_industry", "tlo_profile", 
    "tlo_website", "tlo_profile_completeness", "membership_id"
    )}

  sel_vars <- c(
    "accountid", "toplevelorganization__c", "asset_enddate__c", "asset_startdate__c", 
    "contract_status__c", "name.x", "organization_country_name__c.x", "region__c.x", 
    "primary_engagement_manager_name__c.x", "secondary_engagement_manager__c.x", "name.x.x",
    "organizationprofile__c.x", "website.x", "orgprofile_completeness__c.x", 
    "name.y", "organization_country_name__c.y", "region__c.y", "primary_engagement_manager_name__c.y",
    "secondary_engagement_manager__c.y", "name.y.y", "organizationprofile__c.y", 
    "website.y", "orgprofile_completeness__c.y", "membership_id"
  )

  sel_from_accounts <- c(
    "id", "name", "organization_country_name__c", 
    "region__c", "primary_engagement_manager_name__c", 
    "secondary_engagement_manager__c", "organizationprofile__c",
    "main_industry_sector__c", "website", "orgprofile_completeness__c"
  )

  sel_from_contracts <- c(
    "accountid", "asset_enddate__c", "asset_startdate__c", 
    "contract_status__c")

  tbl_acc <- tbl(conn, dbplyr::in_schema("ods","sfdc_account_v")) %>%
    filter(!ispersonaccount) %>%
    select(all_of(sel_from_accounts))

  tbl_ctr <- tbl(conn, dbplyr::in_schema("ods","sfdc_servicecontract_v")) %>%
    filter(recordtypeid == "012b00000009cmTAAQ" & contract_status__c == "Activated/Running") %>%
    select(all_of(sel_from_contracts))

  industry_details <- tbl(conn, in_schema("ods", "sfdc_industry_v")) %>%
    select(id, name)

  tbl(conn, dbplyr::in_schema("ods","sfdc_membership_v")) %>% 
    filter(community__c %in% community_ids & member_status__c == "Active") %>%
    select(membership_id = "id", accountid = "membername__c", toplevelorganization__c) %>% 
    left_join(tbl_ctr, by = "accountid") %>% 
    left_join(tbl_acc, by = c("accountid" = "id")) %>% 
    left_join(tbl_acc, by = c("toplevelorganization__c" = "id")) %>% 
    left_join(industry_details, by = c("main_industry_sector__c.x" = "id")) %>% 
    left_join(industry_details, by = c("main_industry_sector__c.y" = "id")) %>% 
    select(-c(main_industry_sector__c.x, main_industry_sector__c.y)) %>%
    collect() %>%
    rowwise() %>%
    mutate(orgprofile_completeness__c.x = as.integer(str_sub(orgprofile_completeness__c.x, -3, -2))) %>%
    mutate(orgprofile_completeness__c.y = as.integer(str_sub(orgprofile_completeness__c.y, -3, -2))) %>%
    ungroup() %>%
    select(all_of(sel_vars)) %>%
    rename_all('set_col_names')

}


#' Get TFE and OPS Contacts
#' 
#' Get list of of top level and organisational contacts for a list
#' of organisations.
#' 
#' @description 
#' One needs to provide both organisation ids and names
#' because the main accounts table contains organisation ids but only
#' tlo names.
#' 
#' @param conn connection to the data warehouse
#' @param org_ids list of organisation ids
#' 
#' @examples
#' \dontrun{
#' conn <- connect_dw()
#' dt <- get_all_ncs(conn)
#' get_tlo_contacts(conn, dt$org_id)
#' }
#' 
#' @return tibble
#' 
#' @export
get_tlo_contacts <- function(conn, org_ids) {

  sel_var <- c(
    org_id = "organization__c",
    tlo_id = "top_level_organization__c",
    contact_sfid = "personname__c", 
    contact_position = "name",
    is_top_engagement = "top_forum_engagement__c", 
    is_operational_contact = "operational_contact__c"
  )

  lst_members <- tbl(conn, in_schema("ods", "sfdc_account_v")) %>%  
    select(account_id = "id", person_name = "fullname__c", person_email = "personemail")

  tbl(conn, in_schema("ods", "sfdc_position_v")) %>%
    filter(organization__c %in% org_ids | tlo_id %in% org_ids) %>%
    select(all_of(sel_var)) %>%
    left_join(lst_members, by = c("contact_sfid" = "account_id")) %>%
    filter(is_top_engagement | is_operational_contact) %>%
    collect()

}

#' Get Opportunities
#' 
#' Get the full list of running TLO opportunities
#' 
#' @description 
#' Before engaging with a company or communication about
#' engagement opportunities, it is useful to check existing 
#' opportunities. We select here the following opportunity
#' types: "Approved", "Closing", "Finalist", "Idea", "Proposal", 
#' and "Qualification"
#' 
#' @param conn connection to the data warehouse
#' @param org_ids list of organisation ids
#' 
#' @examples
#' \dontrun{
#' conn <- connect_dw()
#' dt <- get_all_ncs(conn)
#' get_tlo_opportunities(conn, dt$org_id)
#' }
#' 
#' @return tibble
#' 
#' @export
get_tlo_opportunities <- function(conn, org_ids) {

  stage_names <- c(
    "Approved", "Closing", "Finalist", 
    "Idea", "Proposal", "Qualification")

  var_sel <- c(
    "id", "accountid", "stagename", "proposed_category__c", 
    "proposed_membership_type__c", "engagement_level__c", 
    "nextstep", "current_user_sub_team__c", 
    "current_user_team__c", "closedate", "enddate__c"
    )

  tbl(conn, in_schema("ods", "sfdc_opportunity_v")) %>%
    filter(accountid %in% org_ids) %>%
    filter(stagename %in% stage_names) %>%
    select(all_of(var_sel)) %>%
    collect()
  
}

#' Get invoices
#' 
#' Get list of invoices for organisations
#' 
#' @param conn connection to the data warehouse
#' @param org_ids list of organisation ids
#' 
#' @examples 
#' \dontrun{
#' conn <- connect_dw()
#' dt <- get_all_ncs4(conn)
#' get_tlo_invoices(conn, dt$org_id)
#' }
#' 
#' @return a tibble
#' 
#' @export
get_tlo_invoices <- function(conn, org_ids) {

  var_sel <- c(
    "apttus_billing__billtoaccountid__c", "apttus_billing__totalinvoiceamount__c", 
    "apttus_billing__paymentstatus__c", "has_credit_memo__c" ,
    "id", "engagement_level__c","apttus_billing__duedate__c", 
    "payment_date__c")

  tbl(conn, in_schema("ods","apttus_billing_invoice_v")) %>%
    filter(apttus_billing__billtoaccountid__c %in% org_ids) %>%
    select(all_of(var_sel)) %>%
    collect() %>%
    as_tibble()

}

#' Get membership types
#' 
#' Get list of all contract based membership types
#' 
#' @param conn connection to the data warehouse
#' 
#' @examples 
#' \dontrun{
#' conn <- connect_dw()
#' get_contract_membership_types(conn, dt$org_id)
#' }
#' 
#' @return a tibble
#' 
#' @export
get_contract_membership_types <- function(conn) {

  tbl(conn, in_schema("ods","sfdc_servicecontract_v")) %>% 
    select(engagement_level__c, membership_type__c) %>% 
    count(engagement_level__c, membership_type__c) %>% 
    collect() %>%
    arrange(desc(n)) %>%
    mutate(n = as.integer(n))

}


#' Get org constituents
#' 
#' Retrieve all constituents coded under a given
#' organisation.
#' 
#' @description
#' Due to the structure of organisations in SF, one can
#' retrieve the accounts explicitly coded under a given 
#' organisation or all those that fall under each subsidary.
#' 
#' @param conn connection to the data warehouse
#' @param org_id organisation id
#' @param tlo_only only tlo members rather than all subsidaries (default is FALSE)
#' 
#' @examples
#' \dontrun{
#' conn <- connect_dw()
#' org_members(conn, "a0e0X00000hzdENQAY")
#' }
#' 
#' @return a tibble
#' 
#' @export
org_members <- function(conn, org_id, tlo_only = FALSE) {

  tlo_name <- tbl(conn, dbplyr::in_schema("ods","sfdc_account_v")) %>%
    filter(id == org_id) %>%
    select(name) %>%
    collect() %>%
    pull(name)


  if (tlo_only) {

    tbl(conn, dbplyr::in_schema("ods","sfdc_account_v")) %>% 
      filter(status__c == "Active") %>%
      filter(ispersonaccount == TRUE & primaryorganizationid__c == org_id) %>%
      filter(si_sub_type__c == "TopLink") %>%
      select(all_of(var_sel_people)) %>%
      collect() %>%
      mutate(type = "Constituent")

  } else {

    tbl(conn, dbplyr::in_schema("ods","sfdc_account_v")) %>% 
      filter(status__c == "Active") %>%
      filter(ispersonaccount == TRUE & top_level_organization__pc == tlo_name) %>%
      filter(si_sub_type__c == "TopLink") %>%
      select(all_of(var_sel_people)) %>%
      collect() %>%
      mutate(type = "Constituent")

  }

}

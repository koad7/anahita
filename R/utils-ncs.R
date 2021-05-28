
#' @noRd
#' @keywords internal
.all_active_ids <- function(dt) {

  sel <- c("Activated/Running", "Activated/Upcoming")

  tlo_ids <- dt %>%
    filter(contract_status == sel & !is_tlo) %>%
    select(tlo_id) %>%
    pull()
  org_ids <- dt %>%
    filter(contract_status == sel & is_tlo) %>%
    select(org_id) %>%
    pull()
  c(tlo_ids, org_ids)
}

#' @noRd
#' @keywords internal
.all_active_names <- function(dt) {
  tlo_names <- dt %>%
    filter(contract_status == sel & !is_tlo) %>%
    select(tlo_name) %>%
    pull()
  org_names <- dt %>%
    filter(contract_status == sel & is_tlo) %>%
    select(org_name) %>%
    pull()
  c(tlo_names, org_names)
}

#' Retrieve all FME contracts
#' 
#' Retrieve all companies with a Forum Memberhip contracts. We
#' keep all contract status for historical analysis.
#' 
#' @param conn connection to the data warehouse
#' 
#' @examples 
#' \dontrun{
#' conn <- connect_dw()
#' ncs_all_fme_contracts(conn)
#' }
#' 
#' @return a tibble
#' 
#' @export
ncs_all_fme_contracts <- function(conn) {
  get_tlo_contracts(conn, "Membership", "Forum Membership")
}

#' Retrieve all NC contracts
#' 
#' Retrieve all companies with a New Champions contract. We
#' keep all contract status for historical analysis.
#' 
#' @param conn connection to the data warehouse
#' 
#' @examples 
#' \dontrun{
#' conn <- connect_dw()
#' var_sel <- c("accountid","top_level_organization__c")
#' ncs_all_nc_contracts(conn)
#' }
#' 
#' @return a tibble
#' 
#' @export
ncs_all_nc_contracts <- function(conn) {
  get_tlo_contracts(conn, "New Champion", c("Annually", "Quarterly"))
}

#' Retrieve all NC contracts (NCs + FMEs)
#' 
#' Retrieve all companies with a New Champions or FME contract. We
#' keep all contract status for historical analysis.
#' 
#' @param conn connection to the data warehouse
#' @param only_active select only contracts that are "Activated/Running" (default is TRUE)
#' 
#' @examples 
#' \dontrun{
#' conn <- connect_dw()
#' ncs_all_orgs(conn)
#' }
#' 
#' @return a tibble
#' 
#' @export
ncs_all_orgs <- function(conn, only_active = TRUE) {
  
  # Identify FMEs through their contracts
  # Eventually, all renewed contract will fall under the NC community
  
  list_fmes <- ncs_all_fme_contracts(conn) %>%
    mutate(type_contract = "FME")
      
  # Identify the New Champions through contracts
  # NC contracts are established on a yearly basis
  # Note that if a company is a sunsidary of a TLO,
  # than we actually want to surface the TLO. This does not make
  # sense to me. Imagine if we end up with Alphalet?

  list_ncs <-  ncs_all_nc_contracts(conn) %>%
    mutate(type_contract = "NC") %>%
    bind_rows(list_fmes) %>%
    mutate(is_tlo = ifelse(org_id == tlo_id, TRUE, FALSE))
  
  if (!only_active) {
    return(list_ncs)
  } else {
    list_ncs %>%
      filter(contract_status == "Activated/Running" | contract_status == "Activated/Upcoming")
  }
      
}

#' Retrieve all NC companies
#' 
#' Retrieve all NC companies pulling from the contract
#' based communities.
#' 
#' @param conn connection to the data warehouse
#' 
#' @examples 
#' \dontrun{
#' conn <- connect_dw()
#' ncs_all_orgs_by_membership(conn)
#' }
#' 
#' @return a tibble
#' 
#' @export
ncs_all_orgs_by_membership <- function(conn) {

  dt1 <- get_tlo_communities(conn, "a0e0X00000hzfC9QAI") %>%
    mutate(type_contract = "FME")
  dt2 <- get_tlo_communities(conn, "a0eb000000BN4TBAA1") %>%
    mutate(type_contract = "NC")

  bind_rows(dt1, dt2) %>%
    mutate(is_tlo = ifelse(org_id == tlo_id, TRUE, FALSE)) %>%
    relocate(membership_id, .after = last_col())

}

#' Get Org Opportunities
#' 
#' Retrieve list of "open" opportunities.
#' 
#' @description
#' Before engaging with a potential company for a contract renewal or
#' new engagement opportunity, it is useful to check whether that 
#' company has any formal conversations with other teams already. We select
#' opportunities with status "Approved", "Closing", "Finalist", "Idea", "Proposal", 
#' and "Qualification"
#' 
#' @param conn connection to the data warehouse
#' @param org_ids list of organisation ids (default NULL)
#' 
#' @examples
#' \dontrun{
#' conn <- connect_dw()
#' dt <- ncs_all_orgs(conn)
#' ncs_opportunities(conn)
#' ncs_opportunities(conn, dt$org_id)
#' }
#' 
#' @return a tibble
#' 
#' @export
ncs_opportunities <- function(conn, org_ids = NULL) {

  if (is.null(org_ids)) {
    dt <- ncs_all_orgs(conn)
    org_ids <- dt$org_id
  }
  
  get_tlo_opportunities(conn, org_ids)

}

#' Get Org Invoices
#' 
#' Retrieve list of invoices from organisations.
#' 
#' @param conn connection to the data warehouse
#' @param org_ids list of organisation ids (default NULL)
#' 
#' @examples
#' \dontrun{
#' conn <- connect_dw()
#' dt <- ncs_all_orgs(conn)
#' ncs_invoices(conn)
#' ncs_invoices(conn, dt$org_id)
#' }
#' 
#' @return a tibble
#' 
#' @export
ncs_invoices <- function(conn, org_ids = NULL) {

  if (is.null(org_ids)) {
    dt <- ncs_all_orgs(conn)
    org_ids <- dt$org_id
  }
  
  get_tlo_invoices(conn, org_ids)

}

#' Missing companies
#' 
#' Check if companies are missing or need to be
#' added in the NC contract base communities. 
#' 
#' @param conn connection to the data warehouse
#' 
#' @examples 
#' \dontrun{
#' conn <- connect_dw()
#' dt <- ncs_all_orgs(conn)
#' to_add <- dt$to_add
#' to_remove <- dt$to_remove
#' }
#' 
#' @return a named list
#' 
#' @export
ncs_check_missing_companies <- function(conn) {

  list_ncs_contract <- ncs_all_orgs(conn) %>%
    filter(contract_status == "Activated/Running" | contract_status == "Activated/Upcoming")
  
  list_ncs_membership <- ncs_all_orgs_by_membership(conn)

  to_add <- list_ncs_contract %>%
    filter(!org_id %in% list_ncs_membership$org_id)

  to_remove <- list_ncs_membership %>%
    filter(!org_id %in% list_ncs_contract$org_id)

  return(
    list(
      "to_add" = to_add, 
      "to_remove" = to_remove
      )
  )

}

#' Get PEMs
#' 
#' Build the full list of primary and secondary PEMS.
#' 
#' @description 
#' Getting a meaningful PEM list for a list of organisations is slightly
#' convoluted for NCs as you need to look both at the subsidary and tlo
#' level.
#' 
#' @param conn connection to the data warehouse
#' @param dt list of companies to speed up calculations (see `ncs_all_orgs`)
#' 
#' @examples
#' \dontrun{
#' conn <- connect_dw()
#' dt <- ncs_all_orgs(conn)
#' ncs_org_pems(conn)
#' ncs_org_pems(conn, dt)
#' }
#' 
#' @return a tibble
#' 
#' @export
ncs_org_pems <- function(conn, dt = NULL) {

  # Get primary and secondary PEM information
  # Note that the secondary_engagement_manager__c is no the standard id but tech_salesforceuser__c
  # This thing makes no sense, need to ask Arek about it

  if (is.null(dt)) {
    dt <- ncs_all_orgs(conn)
  }
  
  org_ids_pem_1 <- dt %>%
    filter(!is.na(org_pem_1)) %>%
    pull(org_pem_1) 
  
  org_ids_pem_2 <- dt %>%
    filter(!is.na(org_pem_2)) %>%
    pull(org_pem_2)

  tlo_ids_pem_1 <- dt %>%
    filter(!is.na(tlo_pem_1) & !is_tlo) %>%
    pull(tlo_pem_1) 
  
  tlo_ids_pem_2 <- dt %>%
    filter(!is.na(tlo_pem_2) & !is_tlo) %>%
    pull(tlo_pem_2)

  var_sel_pem <- c(
    pem_sfid = "id", 
    pem_tech_sfid = "tech_salesforceuser__c", 
    pem_name = "name", 
    pem_email = "personemail", 
    pem_status = "status__c"
  )

  list_org_pems_1 <- tbl(conn, dbplyr::in_schema("ods","sfdc_account_v")) %>%
    filter(recordtypeid == "012b00000000ARMAA2") %>%
    select(all_of(var_sel_pem)) %>%
    filter(pem_name %in% org_ids_pem_1) %>%
    collect() %>%
    left_join(select(dt, org_id, org_name, org_pem_1), by = c("pem_name" = "org_pem_1")) %>%
    select(org_id, org_name, pem_sfid, pem_name, pem_email, pem_status) %>%
    distinct(org_id, pem_sfid, .keep_all = TRUE) %>%
    mutate(pem_type = "primary") %>%
    rename(tlo_id = "org_id", tlo_name = "org_name")

  list_tlo_pems_1 <- tbl(conn, dbplyr::in_schema("ods","sfdc_account_v")) %>%
    filter(recordtypeid == "012b00000000ARMAA2") %>%
    select(all_of(var_sel_pem)) %>%
    filter(pem_name %in% tlo_ids_pem_1) %>%
    collect() %>%
    left_join(select(dt, tlo_id, tlo_name, tlo_pem_1), by = c("pem_name" = "tlo_pem_1")) %>%
    select(tlo_id, tlo_name, pem_sfid, pem_name, pem_email, pem_status) %>%
    distinct(tlo_id, pem_sfid, .keep_all = TRUE) %>%
    mutate(pem_type = "primary")
    
  list_org_pems_2 <- tbl(conn, dbplyr::in_schema("ods","sfdc_account_v")) %>%
    filter(recordtypeid == "012b00000000ARMAA2") %>%
    select(all_of(var_sel_pem)) %>%
    filter(pem_tech_sfid %in% org_ids_pem_2) %>%
    filter(!is.na(pem_tech_sfid)) %>%
    collect() %>%
    left_join(select(dt, org_id, org_name, org_pem_2), by = c("pem_tech_sfid" = "org_pem_2")) %>%
    select(org_id, org_name, pem_sfid, pem_name, pem_email, pem_status) %>%
    distinct(org_id, pem_sfid, .keep_all = TRUE) %>%
    mutate(pem_type = "secondary") %>%
    rename(tlo_id = "org_id", tlo_name = "org_name")

 list_tlo_pems_2 <- tbl(conn, dbplyr::in_schema("ods","sfdc_account_v")) %>%
    filter(recordtypeid == "012b00000000ARMAA2") %>%
    select(all_of(var_sel_pem)) %>%
    filter(pem_tech_sfid %in% tlo_ids_pem_2) %>%
    filter(!is.na(pem_tech_sfid)) %>%
    collect() %>%
    left_join(select(dt, tlo_id, tlo_name, tlo_pem_2), by = c("pem_tech_sfid" = "tlo_pem_2")) %>%
    select(tlo_id, tlo_name, pem_sfid, pem_name, pem_email, pem_status) %>%
    distinct(tlo_id, pem_sfid, .keep_all = TRUE) %>%
    mutate(pem_type = "secondary")

  bind_rows(
    list_org_pems_1, list_org_pems_2,
    list_tlo_pems_1, list_tlo_pems_2
  ) %>%
  distinct(tlo_id, pem_sfid, .keep_all = TRUE) %>%
  arrange(tlo_id, pem_type)

}

#' Get Contacts
#' 
#' Retrieve TFE and Operational contacts for NC companies.
#' 
#' @param conn connection to the data warehouse
#' @param org_ids list of organisation ids (default NULL)
#' 
#' @examples
#' \dontrun{
#' conn <- connect_dw()
#' dt <- ncs_all_orgs(conn)
#' ncs_contacts(conn)
#' ncs_contacts(conn, dt$org_id)
#' }
#' 
#' @return a tibble
#' 
#' @export
ncs_contacts <- function(conn, org_ids = NULL) {

  if (is.null(org_ids)) {
    dt <- ncs_all_orgs(conn)
    org_ids <- dt$org_id
  }
  
  get_tlo_contacts(conn, org_ids)

}

#' Get people with NC membership
#' 
#' Retrieve all active memberships in the New 
#' Champions constituent community.
#' 
#' @description
#' Because there is no automation to remove people from 
#' the community when a contract is terminated, you can 
#' specify the list of active organisation ids to make sure
#' you reflect number for active companies.
#' 
#' @param conn connection to the data warehouse
#' @param membership_ids list of membership ids
#' @param org_ids list of organisation ids (default NULL)
#' 
#' @examples
#' \dontrun{
#' conn <- connect_dw()
#' dt <- ncs_all_orgs(conn)
#' ncs_members(conn, "a0e0X00000hzdENQAY")
#' ncs_members(conn, "a0e0X00000hzdENQAY", dt$org_id)
#' }
#' 
#' @return a tibble
#' 
#' @export
ncs_members <- function(conn, membership_ids, org_ids = NULL) {

  org_details <- tbl(conn, dbplyr::in_schema("ods","sfdc_account_v")) %>%
    filter(recordtypeid == "012b00000000O7RAAU") %>%
    select(top_level_organization__c = "id", tlo_name = "name")

  tbl_acc <- tbl(conn, dbplyr::in_schema("ods","sfdc_account_v")) %>%
    filter(recordtypeid == "012b00000000ARJAA2") %>%
    select(all_of(var_sel_people))

  dt <- tbl(conn, dbplyr::in_schema("ods","sfdc_membership_v")) %>% 
    filter(community__c == membership_ids & member_status__c == "Active") %>% 
    select(membership_id = "id", id = "membername__c") %>% 
    left_join(tbl_acc, by = "id") %>%
    left_join(org_details, by = c("top_level_organization__pc" = "tlo_name")) %>%
    collect() %>%
    add_count(primaryorganizationid__c) %>%
    arrange(desc(n), primaryorganizationid__c) %>%
    mutate(type = "Member")

  if (is.null(org_ids)) {
    return(dt)
  } else {
    dt <- dt %>%
      filter(primaryorganizationid__c %in% org_ids)
  }

}

#' NC Constituent accounts
#' 
#' Identify consituents within New Champions companies as
#' a primary organization.
#' 
#' @description 
#' We find that not all person accounts under a NC comnpany
#' are part of the New Champions people community. You need to
#' exclude people in `ncs_members` to find the latter.
#'
#' @param conn connection to the data warehouse
#' @param org_ids list of active organisation ids
#' @param people_ids list of people ids to exclude (default is NULL)
#' 
#' @examples
#' \dontrun{
#' conn <- connect_dw()
#' dt <- ncs_all_orgs(conn)
#' ncs_constituents(conn, "a0e0X00000hzdENQAY")
#' ncs_constituents(conn, "a0e0X00000hzdENQAY", dt$org_id)
#' list_members <- ncs_members(conn, "a0e0X00000hzdENQAY", dt$org_id)
#' ncs_constituents(conn, dt$org_id, list_members$id)
#' }
#' 
#' @return a tibble
#' 
#' @export
ncs_constituents <- function(conn, org_ids, people_ids = NULL) {

  org_details <- tbl(conn, dbplyr::in_schema("ods","sfdc_account_v")) %>%
    filter(recordtypeid == "012b00000000O7RAAU") %>%
    select(top_level_organization__c = "id", tlo_name = "name")

  if (!is.null(people_ids)) {
    tbl(conn, dbplyr::in_schema("ods","sfdc_account_v")) %>%
      filter(recordtypeid == "012b00000000ARJAA2") %>%
      filter(status__c == "Active" & has_an_active_position__c > 0) %>% 
      filter(!si_sub_type__c %in% c("Organization", "Personal")) %>%
      filter(primaryorganizationid__c %in% org_ids) %>%
      filter(!id %in% people_ids) %>%
      select(all_of(var_sel_people)) %>%
      left_join(org_details, by = c("top_level_organization__pc" = "tlo_name")) %>%
      collect() %>%
      add_count(primaryorganizationid__c) %>%
      arrange(desc(n), primaryorganizationid__c) %>%
      mutate(type = "Constituent")
  } else {
    tbl(conn, dbplyr::in_schema("ods","sfdc_account_v")) %>%
      filter(recordtypeid == "012b00000000ARJAA2") %>%
      filter(status__c == "Active" & has_an_active_position__c > 0) %>% 
      filter(!si_sub_type__c %in% c("Organization", "Personal")) %>%
      filter(primaryorganizationid__c %in% org_ids) %>%
      select(all_of(var_sel_people)) %>%
      left_join(org_details, by = c("top_level_organization__pc" = "tlo_name")) %>%
      collect() %>%
      add_count(primaryorganizationid__c) %>%
      arrange(desc(n), primaryorganizationid__c) %>%
      mutate(type = "Constituent")
  }

}

#' NC DM accounts
#' 
#' Identify digital members with domains associated to
#' New Champion Companies.
#' 
#' @description 
#' SI users with a domain found to match the domain of a 
#' New Champion company are automatically upgraded to a premium
#' membership which grants them access to open events and other
#' things. Because coding is not 100% clean in SF, we provide an
#' exclusion list of ids for accounts already identified to be 
#' members or consituents for NC companies. This should be
#' simplified in the future. The function uses the whitelisting to
#' overwrite missing primary organisation ids with the organisation
#' associated to the domain in the whitelisting table.
#'
#' @param conn connection to the data warehouse
#' @param org_ids list of active organisation ids
#' @param people_ids list of people ids see `ncs_members` and `ncs_constituents` (default is NULL)
#' 
#' @examples
#' \dontrun{
#' conn <- connect_dw()
#' dt1 <- ncs_all_orgs(conn)
#' list_ncs_members <- ncs_members(conn, "a0e0X00000hzdENQAY")
#' list_ncs_constituents <- ncs_constituents(conn, dt1$org_id)
#' people_ids <- unique(c(list_ncs_members$id, list_ncs_constituents$id))
#' ncs_dms(conn, dt1$org_id, people_ids)
#' }
#' 
#' @return a tibble
#' 
#' @export
ncs_dms <- function(conn, org_ids, people_ids = NULL) {

  dms <- get_dms(conn, org_ids = org_ids, people_ids = people_ids)

  if(nrow(dms$dms) == 0)
    return(dms$dms)
  else
    match_tlo(conn, dms)

}

#' NC PTM accounts
#' 
#' Identify public transformation map users.
#' 
#' @description 
#' By defintion, if the whitelisted domains for our NC
#' companies are complete, there should be no accounts with
#' a public subscription.
#'
#' @param conn connection to the data warehouse
#' @param org_ids list of active organisation ids
#' @param people_ids list of people ids see `ncs_members` and `ncs_constituents` (default is NULL)
#' 
#' @examples
#' \dontrun{
#' conn <- connect_dw()
#' dt <- ncs_all_orgs(conn)
#' ncs_ptms(conn, dt$org_id)
#' }
#' 
#' @return a tibble
#' 
#' @export
ncs_ptms <- function(conn, org_ids, people_ids = NULL) {

  ids <- org_ids
  ptms <- get_ptms(conn, org_ids, people_ids)

  if(nrow(ptms$ptms) == 0) 
    return(ptms$ptms)
  else
    match_tlo(conn, ptms)

}

#' Inactivate Memberships
#' 
#' Identify memberships that should ne inactivated.
#' 
#' @description
#' Because there is no automation to remove people from 
#' the community when a contract is terminated, we need to
#' identify these on a regular basis.
#' 
#' @param conn connection to the data warehouse
#' @param membership_ids list of membership ids
#' @param org_ids list of active organisation ids
#' 
#' @examples
#' \dontrun{
#' conn <- connect_dw()
#' dt <- ncs_all_orgs(conn)
#' ncs_wrong_memberships(conn, "a0e0X00000hzdENQAY")
#' ncs_wrong_memberships(conn, "a0e0X00000hzdENQAY", dt$org_id)
#' }
#' 
#' @return a tibble
#' 
#' @export
ncs_wrong_memberships <- function(conn, membership_ids, org_ids) {

  ncs_members(conn, membership_ids) %>%
    filter(!primaryorganizationid__c %in% org_ids)

}

#' NC anniversaries
#' 
#' Calculate NC quarterly anniversiaries table.
#' 
#' @description
#' Utility function to determine the quarterly anniveraires for
#' active NC companies. This table is at the basis of the quarterly
#' communication app build for the NC team.
#' 
#' @param conn connection to the data warehouse
#' @param dt_ncs tibble of all active nc companies (see `ncs_all_orgs`)
#' 
#' @examples
#' \dontrun{
#' conn <- connect_dw()
#' dt <- ncs_all_orgs(conn)
#' ncs_initialize_tlo_quarterly_anniversaries(conn, dt)
#' }
#' 
#' @return a tibble
#' 
#' @export
ncs_initialize_tlo_quarterly_anniversaries <- function(conn, dt_ncs = NULL) {

  if (is.null(dt_ncs))
    dt_ncs <- ncs_all_orgs(conn)

  dt_ncs %>%
    select(tlo_id, asset_end_date) %>%
    mutate(ann_1 = add_with_rollback(asset_end_date, months(-9), preserve_hms = FALSE)) %>%
    mutate(ann_2 = add_with_rollback(asset_end_date, months(-6), preserve_hms = FALSE)) %>%
    mutate(ann_3 = add_with_rollback(asset_end_date, months(-3), preserve_hms = FALSE)) %>%
    mutate(ann_4 = asset_end_date) %>%
    mutate(diff_1 = as.integer(ann_1 - Sys.Date())) %>%
    mutate(diff_2 = as.integer(ann_2 - Sys.Date())) %>%
    mutate(diff_3 = as.integer(ann_3 - Sys.Date())) %>%
    mutate(diff_4 = as.integer(asset_end_date - Sys.Date())) %>%
    select(-asset_end_date) %>%
    pivot_longer(cols = ann_1:diff_4, names_to = c(".value", "quarter"), names_sep = "_") %>%
    mutate(id = paste0(tlo_id,"_", as.numeric(as.POSIXct(ann)))) %>%
    select(tlo_id, id, quarter, ann, diff)

}

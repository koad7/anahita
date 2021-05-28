#' Retrieve all GI C4IR companies
#' 
#' Retrieve all GI C4IR companies based on
#' contract information.
#' 
#' @description 
#' Retrieve all GI C4IR companies based on
#' contract information. At some point all C4IR
#' contracts will move to the GI engagement level.
#' 
#' @param conn connection to the data warehouse
#' @param only_active select only contracts that are "Activated/Running" (default is TRUE)
#' 
#' @examples
#' \dontrun{
#' conn <- connect_dw()
#' gis_all_orgs_c4ir(conn)
#' }
#' 
#' @return a tibble
#' 
#' @export
gis_all_orgs_c4ir <- function(conn, only_active = TRUE) {

  list <- get_tlo_contracts(
    conn, 
    "4IR Centre Membership", 
    c("Level 1", "Valuation USD250m - USD1B", "4IR Centre Membership"))

  if (only_active) {
    list %>%
      filter(contract_status == "Activated/Running")
  }

}

#' Retrieve all GI companies
#' 
#' Retrieve all GI companies based on
#' contract information.
#' 
#' @description 
#' Retrieve all GI C4IR companies based on
#' contract information. At some point all C4IR
#' contracts will move to the GI engagement level.
#' 
#' @param conn connection to the data warehouse
#' @param only_active select only contracts that are "Activated/Running" (default is TRUE)
#' 
#' @examples
#' \dontrun{
#' conn <- connect_dw()
#' gis_all_orgs_gis(conn)
#' }
#' 
#' @return a tibble
#' 
#' @export
gis_all_orgs_gis <- function(conn, only_active = TRUE) {
  
  list <- get_tlo_contracts(conn, "Global Innovator", "Global Innovator")

  if (only_active) {
    list %>%
      filter(contract_status == "Activated/Running")
  }
}

#' Retrieve all TP companies
#' 
#' Retrieve all TP companies based on
#' contract information.
#' 
#' @description 
#' Note that TPs don't have a contract so we actually 
#' wrap this functoon around the community based
#' query `get_tlo_communities`
#' 
#' @param conn connection to the data warehouse
#' 
#' @examples
#' \dontrun{
#' conn <- connect_dw()
#' gis_all_orgs_tps(conn)
#' }
#' 
#' @return a tibble
#' 
#' @export
gis_all_orgs_tps <- function(conn) {
  
  get_tlo_communities(conn, "a0eb0000000CwUXAA0") %>%
    select(-membership_id)
}

#' Retrieve all GIs
#' 
#' Retrieve all GI companies based on contract 
#' data.
#' 
#' @param conn connection to the data warehouse
#' @param only_active select only contracts that are "Activated/Running" (default is TRUE)
#' 
#' @examples 
#' \dontrun{
#' conn <- connect_dw()
#' gis_all_orgs(conn)
#' }
#' 
#' @return a tibble
#' 
#' @export
gis_all_orgs <- function(conn, only_active = TRUE) {
  
  list_tps <- gis_all_orgs_tps(conn) %>%
    mutate(type_contract = "TP")

  list_c4ir <- gis_all_orgs_c4ir(conn, only_active) %>%
    mutate(type_contract = "GI")
      
  gis_all_orgs_gis(conn, only_active) %>%
    mutate(type_contract = "GI") %>%
    bind_rows(list_c4ir) %>%
    mutate(is_tlo = ifelse(org_id == tlo_id, TRUE, FALSE)) %>%
    bind_rows(list_tps)
      
}

#' Retrieve all GI companies
#' 
#' Retrieve all companies within the Global Innovators
#' SF community.
#' 
#' @description 
#' Retrieving GI companies is done pulling from three different
#' communities in SF. GI here includes Technolohy Pioneers.
#' 
#' @param conn connection to the data warehouse
#' 
#' @examples
#' \dontrun{
#' conn <- connect_dw()
#' gis_all_orgs_by_membership(conn)
#' }
#' 
#' @return a tibble
#' 
#' @export
gis_all_orgs_by_membership <- function(conn) {

  dt1 <- get_tlo_communities(conn, "a0e0X00000GcHiAQAV") %>%
    mutate(type_contract = "GI")
  dt2 <- get_tlo_communities(conn, "a0eb0000000CwUXAA0") %>%
    mutate(type_contract = "TP")

  bind_rows(dt1, dt2) %>%
    mutate(is_tlo = ifelse(org_id == tlo_id, TRUE, FALSE)) %>%
    relocate(membership_id, .after = last_col())

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
#' @param dt list of companies to speed up calculations (see `gis_all_orgs`)
#' 
#' @examples
#' \dontrun{
#' conn <- connect_dw()
#' dt <- gis_all_orgs(conn)
#' gis_org_pems(conn)
#' gis_org_pems(conn, dt)
#' }
#' 
#' @return a tibble
#' 
#' @export
gis_org_pems <- function(conn, dt = NULL) {

  # Get primary and secondary PEM information
  # Note that the secondary_engagement_manager__c is no the standard id but tech_salesforceuser__c
  # This thing makes no sense, need to ask Arek about it

  if (is.null(dt)) {
    dt <- gis_all_orgs(conn)
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

#' Retrieve all GI contacts
#' 
#' Retrieve all contacts from the GI
#' companies.
#' 
#' @param conn connection to the data warehouse
#' @param org_ids list of organisation ids (calculated if not provided)
#' 
#' @examples
#' \dontrun{
#' conn <- connect_dw()
#' dt <- gis_all_orgs(conn)
#' gis_contacts(conn, dt$membername__c)
#' }
#' 
#' @return a tibble
#' 
#' @export
gis_contacts <- function(conn, org_ids = NULL) {

  if (is.null(org_ids)) {
    dt <- gis_all_orgs(conn)
    org_ids <- dt$org_id
  }
  
  get_tlo_contacts(conn, org_ids)

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
#' dt <- gis_all_orgs(conn)
#' gis_opportunities(conn)
#' gis_opportunities(conn, dt$org_id)
#' }
#' 
#' @return a tibble
#' 
#' @export
gis_opportunities <- function(conn, org_ids = NULL) {

  if (is.null(org_ids)) {
    dt <- gis_all_orgs(conn)
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
#' dt <- gis_all_orgs(conn)
#' gis_invoices(conn)
#' gis_invoices(conn, dt$org_id)
#' }
#' 
#' @return a tibble
#' 
#' @export
gis_invoices <- function(conn, org_ids = NULL) {

  if (is.null(org_ids)) {
    dt <- gis_all_orgs(conn)
    org_ids <- dt$org_id
  }
  
  get_tlo_invoices(conn, org_ids)

}

#' Retrieve all GI consituents
#' 
#' Retrieve all constituents from the GI
#' companies.
#' 
#' @description 
#' We simply retrieve all active accounts for which the
#' primary organisation is a GI.
#' 
#' @param conn connection to the data warehouse
#' @param org_ids list of organisation ids (if not provided will run `gis_all_orgs`)
#' 
#' @examples
#' \dontrun{
#' conn <- connect_dw()
#' dt <- gis_all_orgs(conn)
#' gis_members(conn, dt$membername__c)
#' }
#' 
#' @return a tibble
#' 
#' @export
gis_members <- function(conn, org_ids = NULL) {

  if (is.null(org_ids)) {
    dt <- gis_all_orgs(conn)
    org_ids <- unique(dt$org_id)
  }

  tbl(conn, dbplyr::in_schema("ods","sfdc_account_v")) %>%
    filter(ispersonaccount == TRUE & status__c == "Active") %>%
    select(all_of(var_sel_people)) %>%
    filter(primaryorganizationid__c %in% org_ids) %>%
    collect() %>%
    mutate(type = "Constituent")

}

#' Retrieve all GI Digital Members
#' 
#' Retrieve all DMs from the GI companies.
#' 
#' @description 
#' SI users with a domain found to match the domain of a 
#' GI company are automatically upgraded to a premium
#' membership which grants them access to open events and other
#' things. Because coding is not 100% clean in SF, we provide an
#' exclusion list of ids for accounts already identified to be 
#' members or consituents for NC companies. This should be
#' simplified in the future.
#' 
#' @param conn connection to the data warehouse
#' @param org_ids list of organisation ids (if not provided will run `gis_all_orgs`)
#' @param people_ids list of people ids to exclude see `gis_all_orgs` (default is null)
#' 
#' @examples
#' \dontrun{
#' conn <- connect_dw()
#' dt <- gis_all_orgs(conn)
#' gis_members(conn, dt$membername__c)
#' }
#' 
#' @return a tibble
#' 
#' @export
gis_dms <- function(conn, org_ids, people_ids = NULL) {
  
  dms <- get_dms(conn, org_ids = org_ids, people_ids = people_ids)
  
  if(nrow(dms$dms) == 0)
    return(dms$dms)
  else
    match_tlo(conn, dms)
}

#' Retrieve all GI Digital Members
#' 
#' Retrieve all PTMs from the GI companies.
#' 
#' @description 
#' SI users with a domain found to match the domain of a 
#' GI company are automatically upgraded to a premium
#' membership which grants them access to open events and other
#' things. Because coding is not 100% clean in SF, we provide an
#' exclusion list of ids for accounts already identified to be 
#' members or consituents for NC companies. This should be
#' simplified in the future.
#' 
#' @param conn connection to the data warehouse
#' @param org_ids list of organisation ids (if not provided will run `gis_all_orgs`)
#' @param people_ids list of people ids see `nc_members` and `nc_constituents` (default is NULL)
#' 
#' @examples
#' \dontrun{
#' conn <- connect_dw()
#' dt <- gis_all_orgs(conn)
#' gis_members(conn, dt$membername__c)
#' }
#' 
#' @return a tibble
#' 
#' @export
gis_ptms <- function(conn, org_ids, people_ids = NULL) {

  ids <- org_ids
  ptms <- get_ptms(conn, org_ids, people_ids)

  if(nrow(ptms$ptms) == 0) 
    return(ptms$ptms)
  else
    match_tlo(conn, ptms)
}

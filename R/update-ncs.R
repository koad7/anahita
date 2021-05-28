#' Update all NCs tables
#' 
#' @param conn1 connection to the data warehouse
#' @param conn2 connection to the anahita DB
#' @param id NC member community SF id (set to a0e0X00000hzdENQAY)
#' 
#' @examples 
#' \dontrun{
#' conn1 <- connect_dw()
#' conn2 <- connect_anahita()
#' update_ncs_tables(conn1, conn2)
#' }
#' 
#' @export
update_ncs_tables <- function(conn1, conn2, id = "a0e0X00000hzdENQAY") {
  
  cli_h1("Updating New Champions Tables")

  last_updated <- .last_updated()

  cli_process_start("Collecting All New Champion Records")
  list_orgs <- ncs_all_orgs(conn1, FALSE)
  org_ids <- .all_active_ids(list_orgs)
  org_names <- .all_active_names(list_orgs)
  cli_process_done(msg_done = "Collected {.val {nrow(list_orgs)}} TLOs, {.val {length(org_ids)}} with an active contract.")

  cli_process_start("Collecting All New Champion PEMs Info")
  list_pems <- ncs_org_pems(conn1, list_orgs)
  cli_process_done(msg_done = "Collected {.val {nrow(list_pems)}} TLOs PEM relationships, for {.val {length(unique(list_pems$tlo_id))}} organisations.")

  cli_process_start("Collecting All Associated Contacts")
  list_contacts <- ncs_contacts(conn1, org_ids)
  cli_process_done(msg_done = "Collected {.val {nrow(list_contacts)}} contacts for {.val {length(unique(list_contacts$tlo_id))}} TLOs")

  cli_process_start("Collecting All Relevant Opportunities")
  list_opportunities <- ncs_opportunities(conn1, org_ids)
  cli_process_done(msg_done = "Collected {.val {nrow(list_opportunities)}} relevant opportunities")

  cli_process_start("Collecting All Invoice Data")
  list_invoices <- ncs_invoices(conn1, org_ids)
  cli_process_done(msg_done = "Collected {.val {nrow(list_invoices)}} invoices for {.val {length(unique(list_invoices$apttus_billing__billtoaccountid__c))}} TLOs")

  cli_process_start("Collecting All Members")
  list_members <- ncs_members(conn1, id)
  cli_process_done(msg_done = "Found {.val {length(unique(list_members$id))}} members")

  cli_process_start("Collecting All Constituents")
  list_constituents <- ncs_constituents(conn1, org_ids, list_members$id)
  cli_process_done(msg_done = "Found {.val {length(unique(list_constituents$id))}} constituents")

  cli_process_start("Collecting All Digital Members")
  list_dms <- ncs_dms(conn1, org_ids, unique(c(list_members$id, list_constituents$id)))
  cli_process_done(msg_done = "Found {.val {length(unique(list_dms$id))}} digital members")

  cli_process_start("Collecting All PTM users")
  list_ptms <- ncs_ptms(conn1, org_ids, unique(c(list_members$id, list_constituents$id)))
  cli_process_done(msg_done = "Found {.val {length(unique(list_ptms$id))}} ptms")

  cli_process_start("Initializing Anniversaries")
  list_orgs_active <- list_orgs %>% 
    filter(contract_status == "Activated/Running")
  list_anniversaries <- ncs_initialize_tlo_quarterly_anniversaries(conn1, list_orgs_active)
  cli_process_done(msg_done = "Calculated Anniversary Dates")

  cli_process_start("Data Quality Checks")
  list_wrong_members <- ncs_wrong_memberships(conn1, id, org_ids)
  cli_process_done(msg_done = "Finished Data Quality Checks")

  list_people <- bind_rows(
    list_members,
    list_constituents,
    list_dms,
    list_ptms
  )

  cli_process_start("Saving to remote tables")
  dbWriteTable(conn2, "ncs_lastupdated", last_updated, overwrite = TRUE)
  dbWriteTable(conn2, "ncs", list_orgs, overwrite = TRUE)
  dbWriteTable(conn2, "ncs_people", list_people, overwrite = TRUE)
  dbWriteTable(conn2, "ncs_pems", list_pems, overwrite = TRUE)
  dbWriteTable(conn2, "ncs_contacts", list_contacts, overwrite = TRUE)
  dbWriteTable(conn2, "ncs_opportunities", list_opportunities, overwrite = TRUE)
  dbWriteTable(conn2, "ncs_invoices", list_invoices, overwrite = TRUE)
  dbWriteTable(conn2, "ncs_anniversaries", list_anniversaries, overwrite = TRUE)
  dbWriteTable(conn2, "ncs_quality_members", list_wrong_members, overwrite = TRUE)
  cli_process_done(msg_done = "Saved to anahita DB")

}
#' Update all GIs tables
#' 
#' @param conn1 connection to the data warehouse
#' @param conn2 connection to the anahita DB
#' 
#' @examples 
#' \dontrun{
#' conn1 <- connect_dw()
#' conn2 <- connect_anahita()
#' update_gis_tables(conn1, conn2)
#' }
#' 
#' @export
update_gis_tables <- function(conn1, conn2) {

  cli_h1("Updating Global Innovators Tables")

  last_updated <- .last_updated()

  cli_process_start("Collecting All GI Records")
  list_orgs <- gis_all_orgs(conn1)
  org_ids <- list_orgs$org_id
  cli_process_done(msg_done = "Collected {.val {nrow(list_orgs)}} TLOs, {.val {length(org_ids)}} with an active contract.")

  cli_process_start("Collecting All GIs PEMs Info")
  list_pems <- gis_org_pems(conn1, list_orgs)
  cli_process_done(msg_done = "Collected {.val {nrow(list_pems)}} TLOs PEM relationships, for {.val {length(unique(list_pems$tlo_id))}} organisations.")

  cli_process_start("Collecting TLO contacts")
  list_contacts <- gis_contacts(conn1, org_ids)
  cli_process_done(msg_done = "Collected {.val {length(unique(list_contacts$tlo_id))}} TLOs contacts.")

  cli_process_start("Collecting All Relevant Opportunities")
  list_opportunities <- ncs_opportunities(conn1, org_ids)
  cli_process_done(msg_done = "Collected {.val {nrow(list_opportunities)}} relevant opportunities")

  cli_process_start("Collecting All Invoice Data")
  list_invoices <- gis_invoices(conn1, org_ids)
  cli_process_done(msg_done = "Collected {.val {nrow(list_invoices)}} invoices for {.val {length(unique(list_invoices$apttus_billing__billtoaccountid__c))}} TLOs")

  cli_process_start("Collecting All GI Members")
  list_members <- gis_members(conn1, org_ids)
  cli_process_done(msg_done = "Collected {.val {nrow(list_members)}} TLOs Members.")

  cli_process_start("Collecting All GI DMs")
  list_dms <- gis_dms(conn1, org_ids, list_members$id)
  cli_process_done(msg_done = "Collected {.val {nrow(list_dms)}} Digital Members.")

  cli_process_start("Collecting All GI PTMs")
  list_ptms <- gis_ptms(conn1, org_ids)
  cli_process_done(msg_done = "Collected {.val {nrow(list_ptms)}} Public Members.")

  list_people <- bind_rows(
    list_members,
    list_dms,
    list_ptms
  )

  cli_process_start("Saving to remote tables")
  dbWriteTable(conn2, "gis_lastupdated", last_updated, overwrite = TRUE)
  dbWriteTable(conn2, "gis", list_orgs, overwrite = TRUE)
  dbWriteTable(conn2, "gis_pems", list_pems, overwrite = TRUE)
  dbWriteTable(conn2, "gis_contacts", list_contacts, overwrite = TRUE)
  dbWriteTable(conn2, "gis_opportunities", list_opportunities, overwrite = TRUE)
  dbWriteTable(conn2, "gis_invoices", list_invoices, overwrite = TRUE)
  dbWriteTable(conn2, "gis_people", list_people, overwrite = TRUE)
  cli_process_done(msg_done = "Saved to anahita DB")

}
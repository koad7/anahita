
#' Update SF table
#' 
#' @param conn1 connection to the data warehouse
#' @param conn2 connection to the anahita DB
#' 
#' @examples 
#' \dontrun{
#' conn1 <- connect_dw()
#' conn2 <- connect_anahita()
#' update_sf_tables(conn1, conn2)
#' }
#' 
#' @export
update_sf_tables <- function(conn1, conn2) {

  cli_h1("Salesforce")

  # ---- Pull all relevant company information from SF ----
  cli_process_start("Collecting Organisations")
  sf_orgs <- get_sf_orgs(conn1)
  cli_process_done(msg_done = "Collected {.val {nrow(sf_orgs)}} TLOs")

  # ---- Pull all topics ----
  cli_process_start("Collecting Insight Areas")
  sf_topics <- get_sf_topics(conn1)
  cli_process_done(msg_done = "Collected {.val {nrow(sf_topics)}} Insights")

  # ---- Pull all people and hash ----
  cli_process_start("Collecting Active People and Calculate Hash")
  sf_people <- get_sf_people(conn1)
  cli_process_done(msg_done = "Collected {.val {nrow(sf_people)}} Insights")

  cli_process_start("Saving to database")
  dbWriteTable(conn2, "sf_id_table", sf_orgs, overwrite = TRUE)
  dbWriteTable(conn2, "sf_insight_area", sf_topics, overwrite = TRUE)
  dbWriteTable(conn2, "sf_people_hash", sf_people, overwrite = TRUE)
  cli_process_done(msg_done = "Saved to database")

}
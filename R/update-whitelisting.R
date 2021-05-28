#' Update Whitelisting table
#' 
#' @param conn1 connection to the data warehouse
#' @param conn2 connection to the anahita DB
#' 
#' @examples 
#' \dontrun{
#' conn1 <- connect_dw()
#' conn2 <- connect_anahita()
#' update_whitelisting_tables(conn1, conn2)
#' }
#' 
#' @export
update_whitelisting_tables <- function(conn1, conn2) {

  cli_h1("Updating Whitelisting Tables")

  cli_process_start("Collecting All Whitelisted records")
  list_whitelist <- get_whitelisted_domains(conn1)
  cli_process_done(msg_done = "Collected {.val {nrow(list_whitelist)}} active Whitelisting table.")

  cli_process_start("Saving to remote tables")
  dbWriteTable(conn2, "sf_whitelisting", list_whitelist, overwrite = TRUE)
  cli_process_done(msg_done = "Saved to anahita DB")

}

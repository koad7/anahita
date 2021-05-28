#' Update all DMs tables
#' 
#' @param conn1 connection to the data warehouse
#' @param conn2 connection to the anahita DB
#' 
#' @examples 
#' \dontrun{
#' conn1 <- connect_dw()
#' conn2 <- connect_anahita()
#' update_dms_tables(conn1, conn2)
#' }
#' 
#' @export
update_dms_tables <- function(conn1, conn2) {

  cli_h1("Updating Digital Members Tables")

  last_updated <- .last_updated()

  cli_process_start("Collecting All DMs")
  list_dms <- get_dms(conn1)
  cli_process_done(msg_done = "Collected {.val {nrow(list_dms)}} active Digial Members.")

  cli_process_start("Saving to remote tables")
  dbWriteTable(conn2, "dms_lastupdated", last_updated, overwrite = TRUE)
  dbWriteTable(conn2, "dms_people", list_dms$dms, overwrite = TRUE)
  cli_process_done(msg_done = "Saved to anahita DB")

}

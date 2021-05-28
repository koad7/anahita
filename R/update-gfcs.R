#' Update all GFC tables
#' 
#' @param conn1 connection to the data warehouse
#' @param conn2 connection to the anahita DB
#' 
#' @examples 
#' \dontrun{
#' conn1 <- connect_dw()
#' conn2 <- connect_anahita()
#' update_gfcs_tables(conn1, conn2)
#' }
#' 
#' @export
update_gfcs_tables <- function(conn1, conn2) {

  cli_h1("Updating GFC Tables")

  cli_process_start("Collecting All GFCs")
  list_gfc <- gfc_members(conn1)
  cli_process_done(msg_done = "Collected {.val {nrow(list_gfc)}} active GFC Memebers.")

  cli_process_start("Saving to remote tables")
  dbWriteTable(conn2, "gfcs_people", list_gfc, overwrite = TRUE)
  cli_process_done(msg_done = "Saved to anahita DB")

}

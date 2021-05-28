#' Update all EN tables
#' 
#' @param conn1 connection to the data warehouse
#' @param conn2 connection to the anahita DB
#' 
#' @examples 
#' \dontrun{
#' conn1 <- connect_dw()
#' conn2 <- connect_anahita()
#' update_en_tables(conn1, conn2)
#' }
#' 
#' @export
update_en_tables <- function(conn1, conn2) {

  cli_h1("Updating Expert Network Tables")

  cli_process_start("Collecting All ENs")
  list_ens <- en_members(conn1)
  cli_process_done(msg_done = "Collected {.val {nrow(list_ens)}} active Expert Network Memebers.")

  cli_process_start("Saving to remote tables")
  dbWriteTable(conn2, "en_people", list_ens, overwrite = TRUE)
  cli_process_done(msg_done = "Saved to anahita DB")

}

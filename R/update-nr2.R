
#' Update all Nr2 tables
#' 
#' @description
#' The company Nr2 provides us with a list of potential
#' leads for NC engagement in China and South Koarea. The list
#' is refreshed on a monthly basis more or less. The authentication
#' must be refreshed every 7 days.
#' 
#' @param conn connection to anahita DB
#' 
#' @examples 
#' \dontrun{
#' conn <- connect_anahita()
#' update_nr2_tables()
#' }
#' 
#' @export
update_nr2_tables <- function(conn) {

  cli_h1("Updating Nr2 Tables")

  nr2::nr2_auth(config_get(nr2_email), config_get(nr2_secret))

  last_updated <- .last_updated()

  cli_process_start("Collecting All Nr2 leads")
  dt <- nr2::get_all_companies()
  cli_process_done(msg_done = "Found {.val {nrow(dt[[1]])}} companies")

  cli_process_start("Saving to remote tables")
  dbWriteTable(conn, "nr2_lastupdated", last_updated, overwrite = TRUE)
  dbWriteTable(conn, "nr2", dt[[1]], overwrite = TRUE)
  dbWriteTable(conn, "nr2_people", dt[[2]], overwrite = TRUE)
  cli_process_done(msg_done = "Saved to anahita DB")

  return(dt)

}

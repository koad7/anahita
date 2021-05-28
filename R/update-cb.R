#' Update CB table
#' 
#' @description 
#' Updating the CrunchBase data set includes the dowlaod
#' of mutlitple csv files. These are stored in a temp folder.
#' The files are loaded and wrangled before associated tables
#' are pushed to the anahita DB.
#' 
#' @param conn connection to the anahita DB
#' @param cb_token CrunchBase auth token (by default assumes available in config file)
#' 
#' @examples 
#' \dontrun{
#' conn <- connect_anahita()
#' update_cb_tables(conn1)
#' }
#' 
#' @export
update_cb_tables <- function(conn, cb_token = config_get(cb_token)) {

  cli::cli_h1("Crunchbase")

  if (is.null(cb_token))
    stop("CB token undefined. Pass a token or check your config file.")

  dir <- cb_download_zip(cb_token)
  cb_push_data(conn, dir)

}

#' Matching Procedure
#' 
#' Company matching procedure
#' 
#' @description 
#' The company matching procedure is the backbone of the
#' company intelligence we intend to provide. The procedure
#' is intended to involve to progressively improve the matching
#' results. To this date, it is a very conservative approach that
#' uses exact matches on website URLs, company record names and
#' company legal names.
#' 
#' @param conn connection to the anahita DB
#' 
#' @examples 
#' \dontrun{
#' conn <- connect_anahita()
#' match_all(conn)
#' }
#' 
#' @export
update_master_tables <- function(conn) {
  
  cli_h1("Starting Matching Procedure")

  # step 1 - unambiguous match to start
  match_to_name_plus_domain(conn)

  # step 2 - match only to domain
  match_to_domain(conn)

  # step 3 - match only to domain
  match_to_legal_name(conn)

  # step 4 - Add missing companies
  add_missing_salesforce(conn)

  # informative message
  update_log(conn)

}



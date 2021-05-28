#' Get Global Future Councils
#' 
#' Get all active GFC members
#' 
#' @param conn connection to the data warehouse
#' 
#' @examples
#' \dontrun{
#' conn <- connect_dw()
#' gfc_members(conn)
#' }
#' 
#' @return tibble
#' 
#' @export
gfc_members <- function(conn) {

  tbl_acc <- tbl(conn, dbplyr::in_schema("ods","sfdc_account_v")) %>%
    filter(recordtypeid == "012b00000000ARJAA2") %>%
    select(all_of(var_sel_people))

  tbl(conn, in_schema("ods", "sfdc_membership_v")) %>% 
    filter(network__c == "a0eb0000008KdpJAAS" & member_status__c == "Active") %>% 
    select(id = "membername__c", category__c, community_name = "name") %>%
    left_join(tbl_acc, by = "id") %>%
    collect() %>%
    relocate(category__c, .after = last_col()) %>%
    relocate(community_name, .after = last_col()) %>%
    rowwise() %>%
    mutate(community_name = strsplit(community_name, "-")[[1]][1]) %>%
    ungroup()

}
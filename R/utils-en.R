#' Get Expert Network Members
#' 
#' Get all active expert network members
#' 
#' @param conn connection to the data warehouse
#' 
#' @examples
#' \dontrun{
#' conn <- connect_dw()
#' en_members(conn)
#' }
#' 
#' @return tibble
#' 
#' @export
en_members <- function(conn) {

  # sel_vars <- c(
  #   "id", "name", "personemail", "short_profile__c", "formal_profile__c", 
  #   "insigth_account_id", "topic_id", "topic_name", "topic_title", "topic_type", 
  #   "topic_status", "is_primary_expertise__c"
  # )

  tbl_acc <- tbl(conn, dbplyr::in_schema("ods","sfdc_account_v")) %>%
    filter(recordtypeid == "012b00000000ARJAA2") %>%
    select(all_of(var_sel_people))

  # tbl_top <- tbl(conn, dbplyr::in_schema("ods","sfdc_topic_v")) %>%
  #   select(
  #     topic_id = "id", 
  #     topic_name = "name", 
  #     topic_title = "publishable_title__c", 
  #     topic_type = "type__c",
  #     topic_cluster = "cluster__c",
  #     topic_status = "status__c")

  tbl(conn, dbplyr::in_schema("ods","sfdc_membership_v")) %>%
    filter(community__c == "a0eb0000008f6LRAAY" & member_status__c == "Active") %>%
    select(id = "membername__c") %>%
    left_join(tbl_acc, by = "id") %>%
    collect()

  # ids <- dt_en$id

  # dt_expertise <- tbl(con_dw, dbplyr::in_schema("ods","sfdc_topic_expert_v")) %>%
  #   filter(experts__c %in% ids) %>%
  #   select(insigth_account_id = "id", id = "experts__c", topic_id = "topic__c", is_primary_expertise__c) %>%
  #   left_join(tbl_top, by = "topic_id") %>%
  #   filter(!is.na(topic_type)) %>%
  #   collect() %>%
  #   left_join(dt_en, by = "id") %>%
  #   select(all_of(sel_vars))

}

#' Get Experts by Insights
#' 
#' Get all active expert network members with expertise
#' 
#' @param conn connection to the data warehouse
#' @param ins_id vector saleforce insight area id
#' 
#' @examples
#' \dontrun{
#' conn <- connect_dw()
#' en_experts_by_insights(conn)
#' }
#' 
#' @return tibble
#' 
#' @export
en_experts_by_insights <- function(conn, ins_id = NULL) {

  sel_vars <- c(
    "id", "name", "personemail", "insigth_account_id", 
    "topic_id", "topic_name", "topic_title", "topic_type", 
    "topic_status", "is_primary_expertise__c"
  )

  tbl_acc <- tbl(conn, dbplyr::in_schema("ods","sfdc_account_v")) %>%
    filter(recordtypeid == "012b00000000ARJAA2") %>%
    select(all_of(var_sel_people))

  tbl_top <- tbl(conn, dbplyr::in_schema("ods","sfdc_topic_v")) %>%
    select(
      topic_id = "id", 
      topic_name = "name", 
      topic_title = "publishable_title__c", 
      topic_type = "type__c",
      topic_cluster = "cluster__c",
      topic_status = "status__c")

  dt_en <- tbl(conn, dbplyr::in_schema("ods","sfdc_membership_v")) %>%
    filter(community__c == "a0eb0000008f6LRAAY" & member_status__c == "Active") %>%
    select(id = "membername__c") %>%
    left_join(tbl_acc, by = "id") %>%
    collect()

  ids <- dt_en$id

  if (is.null(ins_id)) {
    tbl(conn, dbplyr::in_schema("ods","sfdc_topic_expert_v")) %>%
      filter(experts__c %in% ids) %>%
      select(insigth_account_id = "id", id = "experts__c", topic_id = "topic__c", is_primary_expertise__c) %>%
      left_join(tbl_top, by = "topic_id") %>%
      filter(!is.na(topic_type)) %>%
      collect() %>%
      left_join(dt_en, by = "id") %>%
      select(all_of(sel_vars))
  } else {
    tbl(conn, dbplyr::in_schema("ods","sfdc_topic_expert_v")) %>%
      filter(experts__c %in% ids) %>%
      select(insigth_account_id = "id", id = "experts__c", topic_id = "topic__c", is_primary_expertise__c) %>%
      left_join(tbl_top, by = "topic_id") %>%
      filter(!is.na(topic_type)) %>%
      filter(topic_id %in% ins_id) %>%
      collect() %>%
      left_join(dt_en, by = "id") %>%
      select(all_of(sel_vars))
  }

}


#' Get Expert Network Organisations
#' 
#' @description
#' Expert Network members are in theory always affiliated with an
#' exisitng organisation in Salesforce. We use this primary organisation
#' to build the list of all organisation directly (sometimes it is their
#' only engagement point with the Forum) or indirectly involved with the
#' Expert Network.
#' 
#' @param conn connection to the data warehouse
#' @param org_ids vector of org ids (see `en_members`)
#' 
#' @examples
#' \dontrun{
#' conn <- connect_dw()
#' en_orgs(conn)
#' ens <- en_members(conn)
#' en_orgs(conn, ens$org_id)
#' }
#' 
#' @return tibble
#' 
#' @export
en_orgs <- function(conn, org_ids = NULL) {

  if (is.null(org_ids)) {
    org_ids <- en_members(conn) %>%
      filter(!is.na(primaryorganizationid__c)) %>%
      pull(primaryorganizationid__c)
  }

  names_var <- function(.) {
    c("org_id", "org_name", 
    "org_country", "org_region", "org_pem_1", 
    "org_website", "org_profile", "billingcity", "city1__c",              
    "tech_orgfulladdress__c", "org_industry", "type_contract")
  }

  industry_details <- tbl(conn, in_schema("ods", "sfdc_industry_v")) %>%
    select(id, name)

  tbl(conn, dbplyr::in_schema("ods","sfdc_account_v")) %>%
    select(id, name, organization_country_name__c, region__c, primary_engagement_manager_name__c, website, main_industry_sector__c, organizationprofile__c, billingcity, city1__c, tech_orgfulladdress__c) %>%
    filter(id %in% org_ids) %>%
    left_join(industry_details, by = c("main_industry_sector__c" = "id")) %>%
    select(-main_industry_sector__c) %>%
    collect() %>%
    distinct(id, .keep_all = TRUE) %>%
    mutate(type_contract = "EN") %>%
    rename_all('names_var') %>%
    select(type_contract, org_id, org_name, org_country, org_region, org_pem_1, org_website, org_industry, org_profile, billingcity, city1__c, tech_orgfulladdress__c)

}
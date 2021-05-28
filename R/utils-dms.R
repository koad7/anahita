#' Digital Members
#' 
#' Identify different groups of digital members.
#' 
#' @description 
#' The Strategic Intelligence Subscription Type is used to bucket categorise accounts for various 
#' purposes, primarily for account management and email distribution.  It is acknowledged that 
#' accounts may legitimately belong to multiple types, however this is used to identify their 
#' primary type. Subscription Type can be one of: None, Personal, Organization, TopLink
#' Rules as to which subscription type a person has are applied in the following order:
#' 1. If the account is inactive, SI_Sub_Type__c = None
#' 2. If the person has a Subscription Level of Public, SI_Sub_Type__c = None
#' 3. If the person has a qualifying personal engagement: SI_Sub_Type__c = TopLink
#' 4. If the person has a qualifying position with a member or partner organization: SI_Sub_Type__c = TopLink
#' 5. If the person is a Forum Staff Member: SI_Sub_Type__c = TopLink
#' 6. If the TopLink eligibility has been directly overridden as Eligible on the account: SI_Sub_Type__c = TopLink
#' 7. If the person has a paying subscription: SI_Sub_Type__c = Personal
#' 8. If the SI Subscription level has been directly overridden on the account: SI_Sub_Type__c = Personal
#' 9. If the person has a TopLink Username using the domain of a members or partner organization: SI_Sub_Type__c = Organization 
#'
#' @param conn connection to the data warehouse
#' @param sub_types vector of DM types to retrieve (can be Personal, Organization or both)
#' @param mem_types vector of DM membership tiers (can be None, Public, Premium, Pro, Experimental, Platinum)
#' @param org_ids vector of active organisation ids
#' @param people_ids vector of user SF ids you might want to exclude
#' 
#' @examples
#' \dontrun{
#' conn <- connect_dw()
#' dt1 <- get_all_ncs(conn)
#' get_dms(conn, dt1$org_id)
#' get_dms(conn, "Personal", "Pro")
#' }
#' 
#' @return a named list
#' 
#' @export
get_dms <- function(
  conn, 
  sub_types =  c("Personal", "Organization"), 
  mem_types = c("None", "Public", "Premium", "Pro", "Experimental", "Platinum"), 
  org_ids = NULL,
  people_ids = NULL) {

  if (!is.null(people_ids)) {
    dt <- tbl(conn, dbplyr::in_schema("ods","sfdc_account_v")) %>% 
      filter(ispersonaccount == TRUE & status__c == "Active" & (has_onboarded__c == TRUE | recordtypeid == "012b0000000cgBxAAI")) %>%
      filter(si_sub_type__c %in% sub_types) %>%
      filter(si_sub_final_text__c %in% mem_types) %>%
      filter(!id %in% people_ids) %>%
      select(all_of(var_sel_people)) %>%
      collect() %>%
      rowwise() %>%
      mutate(domain = strsplit(personemail, "@")[[1]][2]) %>%
      ungroup() %>%
      mutate(type = "Digital Member") 
  } else {
    dt <- tbl(conn, dbplyr::in_schema("ods","sfdc_account_v")) %>% 
      filter(ispersonaccount == TRUE & status__c == "Active" & (has_onboarded__c == TRUE | recordtypeid == "012b0000000cgBxAAI")) %>%
      filter(si_sub_type__c %in% sub_types) %>%
      filter(si_sub_final_text__c %in% mem_types) %>%
      select(all_of(var_sel_people)) %>%
      collect() %>%
      rowwise() %>%
      mutate(domain = strsplit(personemail, "@")[[1]][2]) %>%
      ungroup() %>%
      mutate(type = "Digital Member") 
  }
  

  if (is.null(org_ids)) {
    whitelisting <- get_whitelisted_domains(conn)
    return(list("dms" = dt, "whitelist" =  whitelisting))
  } else {
    whitelisting <- get_whitelisted_domains(conn, org_ids)
    dt <- dt %>% filter(domain %in% whitelisting$email_domain__c)
    return(list("dms" = dt, "whitelist" =  whitelisting))
  }

}


#' Digital Members Companies
#' 
#' Identify the DM company based on whitelsiting
#' 
#' @description 
#' Many SI users are auto-upgraded to premium SI membership because
#' their email domain is whitelisted as belonging to a company with
#' an active Forum engagement. To keep the format of the table returned
#' by `get_dms` bindable with other people account methods, we provide 
#' another utility function to retrieve the company. 
#'
#' @param conn connection to the data warehouse
#' @param tbl_dms a dm tibble returned by `get_dms`
#' 
#' @examples
#' \dontrun{
#' conn <- connect_dw()
#' dms <- get_dms(conn)
#' get_dms_companies(conn, dms)
#' }
#' 
#' @return a tibble
#' 
#' @export
get_dms_companies <- function(conn, tbl_dms) {

  dt <- tbl(conn, dbplyr::in_schema("ods","sfdc_account_v")) %>% 
    filter(ispersonaccount == TRUE & status__c == "Active" & (has_onboarded__c == TRUE | recordtypeid == "012b0000000cgBxAAI")) %>%
    filter(si_sub_type__c %in% sub_types) %>%
    filter(si_sub_final_text__c %in% mem_types) %>%
    select(all_of(var_sel_people)) %>%
    collect() %>%
    rowwise() %>%
    mutate(domain = strsplit(personemail, "@")[[1]][2]) %>%
    ungroup() %>%
    mutate(type = "Digital Member") 

  if (is.null(org_ids)) {
    return(dt)
  } else {
    whitelisting <- get_whitelisted_domains(conn, org_ids)
    dt %>%
      filter(domain %in% whitelisting$email_domain__c)
  }

}


#' DMs through SI engagement
#' 
#' @description 
#' Some partners have officially chosen SI as
#' an engagement point. Their employees are auto-upgraded
#' auto-upgraded DMs in virtue of that. 
#'
#' @param conn connection to the data warehouse
#' 
#' @examples
#' \dontrun{
#' conn <- connect_dw()
#' dms <- get_dms(conn)
#' get_dms_companies(conn, dms)
#' }
#' 
#' @return a tibble
#' 
#' @export
get_dms_si_engagement <- function(conn) {

  list_si_orgs <- tbl(conn, dbplyr::in_schema("ods","sfdc_membership_v")) %>% 
    filter(community__c == "a0eb0000008Els8AAC" & member_status__c == "Active") %>% 
    collect()

  dt <- get_dms(
    conn, 
    sub_types =  c("Personal", "Organization"), 
    mem_types = c("None", "Public", "Premium", "Pro", "Experimental", "Platinum"), 
    org_ids = list_si_orgs$toplevelorganization__c)

  return(dt)

}


#' Public Transformation Map Users
#' 
#' Identify different groups of digital members.
#' 
#' @description 
#' We define Public Transformation Map users as SI user with a Public subscription level. These
#' are mostly Lite account types but also a few consituents with no active positions.
#'
#' @param conn connection to the data warehouse
#' @param org_ids list of active organisation ids (default = NULL)
#' @param people_ids vector of user SF ids you might want to exclude (default = NULL)
#' 
#' @examples
#' \dontrun{
#' conn <- connect_dw()
#' dt1 <- get_all_ncs(conn)
#' get_ptms(conn, dt1$org_id)
#' }
#' 
#' @return a tibble
#' 
#' @export
get_ptms <- function(conn, org_ids = NULL, people_ids = NULL) {

  dt <- tbl(conn, dbplyr::in_schema("ods","sfdc_account_v")) %>% 
    filter(ispersonaccount == TRUE & status__c == "Active" & (has_onboarded__c == TRUE | recordtypeid == "012b0000000cgBxAAI")) %>%
    filter(si_sub_type__c == "None") %>%
    filter(si_sub_final_text__c == "Public") %>%
    select(all_of(var_sel_people)) %>%
    collect() %>%
    rowwise() %>%
    mutate(domain = strsplit(personemail, "@")[[1]][2]) %>%
    ungroup() %>%
    mutate(type = "Public Users") 

  if (is.null(org_ids)) {
    whitelisting <- get_whitelisted_domains(conn)
    return(list("ptms" = dt, "whitelist" =  whitelisting))
  } else {
    whitelisting <- get_whitelisted_domains(conn, org_ids)
    dt <- dt %>% filter(domain %in% whitelisting$email_domain__c)
    return(list("ptms" = dt, "whitelist" =  whitelisting))
  }

}


#' Signups through SI
#' 
#' Build milestone metric aroudn signup
#' 
#' @description 
#' We define the total number of Public SI users as the number of people
#' who signed-up to SI, irrespective of status and constituency type.
#' 
#' @param conn connection to the data warehouse
#' 
#' @examples
#' \dontrun{
#' conn <- connect_dw()
#' dt1 <- get_si_signups(conn)
#' }
#' 
#' @return a tibble
#' 
#' @export
get_si_signups <- function(conn) {

  var_sel_signups <- c(
    "id", "name", "age__c", "firstname", "lastname", "status__c", "personemail",
    "toplink_account_status_formula__c", "toplink_last_activity_date__c",
    "si_sub_final_text__c", "si_sub_type__c", "accountsource", "stripe_id__c",
    "createddate", "newsletter_subscription__c", "personhasoptedoutofemail", 
    "unsubscribed_in_mc__c", "contactstatus__c", "accountsource"
  )

  dt <- tbl(conn, dbplyr::in_schema("ods","sfdc_account_v")) %>% 
    filter(ispersonaccount == TRUE & (has_onboarded__c == TRUE | recordtypeid == "012b0000000cgBxAAI")) %>%
    filter(!grepl("weforum.org", personemail)) %>%  
    filter(!grepl("serendipityai.co", personemail)) %>%
    filter(status__c == "Active") %>%
    filter(accountsource == "Digital Membership Portal") %>%
    select(all_of(var_sel_signups)) %>%
    collect() %>%
    rowwise() %>%
    mutate(domain = strsplit(personemail, "@")[[1]][2]) %>%
    ungroup()

}
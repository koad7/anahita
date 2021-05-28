#' Update stripe customers
#' 
#' @description 
#' This daily automation crawls the data from stripe and
#' matches customers with various sources of data such as
#' salesforce and crunchbase.
#' 
#' @param dev deployment status read from config (dev will not right into DBs)
#' @param stripe_key stripe api key
#' 
#' @examples 
#' \dontrun{
#' update_stripe_table()
#' }
#' 
#' @return tibble
#' 
#' @export
update_stripe_table <- function(dev = config_get(deploy_status), stripe_key = config_get(stripe)) {

  cat("\n\n########### Update stripe table ###########\n")
  cat("---> Deployment status = ", dev, "\n\n")

  con_dw <- connect_dw()
  con_an <- connect_anahita()
  #con_dm <- connect_dm()

  on.exit({
    cat("Closing connections")
    DBI::dbDisconnect(con_dw)
    DBI::dbDisconnect(con_an)
    #DBI::dbDisconnect(con_dm)
    add = TRUE
    })

  cat("1. Fetching Stripe data from API\n")
  stripe <- stripe_fetch_data(stripe_key)
  sf_ids <- stripe$id

  cat("2. Pulling Salesforce data\n")

  # ids for search
  sf <- tbl(con_dw, in_schema("ods", "sfdc_account_v")) %>%
    filter(id %in% sf_ids) %>%
    select(id, firstname, lastname, personemail, toplink_username__c, createddate, newsletter_subscription__c, personhasoptedoutofemail, unsubscribed_in_mc__c, contactstatus__c) %>%
    collect()

  # merge
  stripe <- stripe %>%
    left_join(sf, by = "id") %>% 
    arrange(desc(stripe_create)) %>% 
    mutate(status = ifelse(is.na(status), "incomplete", status)) %>% 
    filter(stripe_create > as.Date("2019-11-10"))

  cat("3. Pulling TopList information\n")

  toplist <- tbl(con_dw, in_schema("ods", "sfdc_account_v")) %>%
    filter(grepl("TopList Business", account_value_network_roles__c)) %>%
    select(id, organisation = "name") %>%
    collect()

  mapping <- tbl(con_an, "domain_to_org_mapping") %>%
    collect()

  toplist <- inner_join(mapping, toplist, by = "organisation") %>% 
    select(domain) %>% 
    mutate(is_toplist = TRUE)

  stripe$domain <- urltools::domain(stripe$personemail) %>% 
    urltools::suffix_extract() %>% 
    pull(domain)
  
  stripe$host <- urltools::domain(stripe$personemail)

  stripe <- left_join(
    stripe, 
    mapping, 
    by = "domain"
  )

  stripe <- left_join(stripe, toplist, by = "domain") %>% 
    mutate(
      is_toplist = case_when(
        is.na(is_toplist) ~ FALSE,
        TRUE ~ is_toplist
      )
    )

  cat("4. Anahita company matching\n")

  generic_hosts <- tbl(con_an, "domain_generic_domains") %>%
    collect()

  hosts <- stripe %>% 
    select(host) %>% 
    distinct() %>% 
    anti_join(generic_hosts, by = "host") %>% 
    filter(!is.na(host)) %>% 
    pull(host)

  cb <- tbl(con_an, "cb_organizations") %>%
    filter(domain %in% hosts) %>%
    collect() %>%
    group_by(domain) %>% 
    slice(1) %>% 
    ungroup()

  names(cb) <- paste0("cb_", names(cb))

  stripe <- left_join(stripe, cb, by = c("host" = "cb_domain"))

  generic_hosts$email_type <- "generic"

  stripe <- stripe %>% 
    left_join(generic_hosts, by = "host") %>% 
    mutate(
      email_type = case_when(
        is.na(email_type) & grepl("\\.edu|\\.etu|\\.ac", personemail) ~ "academic",
        is.na(email_type) ~ "business",
        TRUE ~ email_type
      )
    )

  if (dev == "prod") {
    cat("5. Pushing table to server\n")
    dbWriteTable(con_an, "stripe_customers", stripe, append = FALSE, overwrite = TRUE)
    #dbWriteTable(con_dm, "stripe", stripe, append = FALSE, overwrite = TRUE)
  }

  return(stripe)

  cat("\n\n########### Update procedure completed ###########\n")

}

#' Update stripe customers
#' 
#' @description 
#' This feeds a DB with a single table used by Elvin to
#' report numbers on the Business Performance reporting side.
#' 
#' @param dev deployment status read from config (dev will not right into DBs)
#' 
#' @examples 
#' \dontrun{
#' update_stats_for_elvin()
#' }
#' 
#' @return tibble
#' 
#' @export
update_stats_for_elvin <- function(dev = config_get(deploy_status)) {

  cat("\n\n########### Update stats Elvin table ###########\n")
  cat("---> Deployment status = ", dev, "\n\n")

  con_bs <- connect_bs()
  con_an <- connect_anahita()
  con_dw <- connect_dw()

  on.exit({
    cat("Closing connections")
    DBI::dbDisconnect(con_dw)
    DBI::dbDisconnect(con_an)
    DBI::dbDisconnect(con_bs)
    add = TRUE
    })

  df <- stripe_stats_for_elvin(con_an, con_dw)

  cat("---> 1.1 Saving", "\nPTM DM:", df$ptm_dm, "\nTotal DM:", df$total_dm, "\nPro:", df$total_pro, "\n")

  if (dev == "prod") {
    cat("2. Pushing table to server\n")
    dbWriteTable(con_bs, "digital-members", df, overwrite = TRUE, append = FALSE)
  }

  return(df)
}
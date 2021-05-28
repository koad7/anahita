#' Retrieve Stripe Customers
#' 
#' @param conn connection to the anahita Data Base
#' 
#' @examples
#' \dontrun{
#' conn <- connect_anahita()
#' stripe_customers(conn)
#' }
#' 
#' @return tibble
#' 
#' @export
stripe_customers <- function(conn) {

  tbl(conn, "stripe_customers") %>% 
    filter(!id %in% stripe_excl_ids) %>%
    collect()

}

#' Retrieve abandonned payments
#' 
#' @param conn connection to the anahita Data Base
#' 
#' @examples
#' \dontrun{
#' conn <- connect_anahita()
#' stripe_abandonned_payments(conn)
#' }
#' 
#' @return tibble
#' 
#' @export
stripe_abandonned_payments <- function(conn) {

  stripe <- stripe_customers(conn)

  ids_incomplete <- stripe %>% 
    filter(status == "incomplete") %>% 
    pull(id)

  stripe %>%
    filter(id %in% ids_incomplete) %>%
    add_count(id) %>%
    filter(n == 1)

}

#' Fetch Stripe Customers
#' 
#' @param stripe_key stripe API key
#' 
#' @examples
#' \dontrun{
#' stripe_key = config_get(stripe)
#' stripe_fetch_data(stripe_key)
#' }
#' 
#' @return tibble
#' 
#' @export
stripe_fetch_data <- function(stripe_key) {

  if (is.null(stripe_key))
    stop("Stripe key was not found")

  striper::set_key(stripe_key)
  customers <- striper::get_customers()
  subs <- striper::get_subscriptions()
  canceled <- striper::get_subscriptions(status = "canceled")

  customer_ids <- purrr::map(customers, "id") %>% 
    unlist()

  sf_ids <- purrr::map(customers, "metadata") %>% 
    purrr::map("salesforce_id") %>% 
    unlist()

  delinquent <- purrr::map(customers, "delinquent") %>% 
    unlist()

  created <- purrr::map(customers, "created") %>% 
    unlist() %>% 
    as.POSIXct(origin = "1970-01-01") 

  stripe_create <- as.Date(created)

  stripe <- tibble::tibble(
    customer_ids = customer_ids,
    id = sf_ids,
    delinquent = delinquent,
    stripe_create = stripe_create,
    stripe_create_time = created
  )

  # subs data
  subs <- append(subs, canceled)

  subs_ids <- purrr::map(subs, "customer") %>% 
    unlist()

  status <- purrr::map(subs, "status") %>% 
    unlist()

  cancel <- purrr::map(subs, function(x){
    !is.null(x$cancel_at)
  }) %>% 
    unlist()

  plan <- purrr::map(subs, "plan") %>% 
    purrr::map(function(x){
      if(!is.null(x$metadata))
        x$metadata
      else
        NA
    }) %>% 
    purrr::map(function(x){
      if(!is.null(x$level))
        x$level
      else
        NA
    }) %>% 
    unlist()

  canceled_date <- purrr::map(subs, function(x){
    if(!is.null(x$canceled_at))
      x$canceled_at
    else
      NA
  }) %>% 
    unlist() %>% 
    as.POSIXct(origin = "1970-01-01") 

  cancel_at <- purrr::map(subs, function(x){
    if(!is.null(x$cancel_at))
      x$cancel_at
    else
      NA
  }) %>% 
    unlist() %>% 
    as.POSIXct(origin = "1970-01-01") 

  current_period_start <- purrr::map(subs, "current_period_start") %>% 
    unlist() %>% 
    as.POSIXct(origin = "1970-01-01") 

  current_period_end <- purrr::map(subs, "current_period_end") %>% 
    unlist() %>% 
    as.POSIXct(origin = "1970-01-01") 

  subscriptions <- tibble::tibble(
    customer_ids = subs_ids,
    status = status,
    plan = plan,
    to_cancel = cancel,
    cancel_at = cancel_at,
    canceled_date = canceled_date,
    current_period_start = current_period_start,
    current_period_end = current_period_end
  )

  stripe <- left_join(stripe, subscriptions, by = "customer_ids") %>% 
    dplyr::group_by(customer_ids) %>% 
    dplyr::slice(1) %>% 
    dplyr::ungroup()

}


#' Business performance reporting
#' 
#' @param conn1 connection anahita
#' @param conn2 connection dw
#' 
#' @examples
#' \dontrun{
#' conn1 <- connect_anahita()
#' conn2 <- connect_dw()
#' stripe_stats_for_elvin(stripe_key)
#' }
#' 
#' @return tibble
#' 
#' @export
stripe_stats_for_elvin <- function(conn1, conn2) {

  stripe <- stripe_customers(conn1)
  
  status_sel <- c("active", "past_due", "trialing")

  ptm_dm <- stripe %>%
    filter(plan == "premium") %>%
    filter(status %in% status_sel) %>%
    count()

  total_pro <- stripe %>%
    filter(plan == "pro") %>%
    filter(status %in% status_sel) %>%
    count()

  total_dm <- tbl(conn2, in_schema("ods", "sfdc_account_v")) %>%
    filter(has_onboarded__c == "TRUE") %>%
    filter(si_sub_type__c == "Organization") %>%
    filter(si_sub_final_text__c == "Premium" | si_sub_final_text__c == "TopLink") %>%
    count() %>%
    collect()

  tibble::tibble(
    ptm_dm = as.integer(ptm_dm$n),
    total_dm = as.integer(total_dm$n),
    total_pro = as.integer(total_pro$n)
  )

}
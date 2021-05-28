
#' Stakeholder domain mapping
#' 
#' @description 
#' We're interested in mapping users into various
#' stakeholder groups. We can use various email domain 
#' lists to do that.
#'
#' @param conn1 <- connect_anahita()
#' @param emails vector of emails
#' 
#' @examples
#' \dontrun{
#' conn1 <- connect_anahita()
#' emails <- c("ddd@edu.com", "ssss@gmail.com")
#' df <- assign_domain_type(conn1, dt)
#' }
#' @return a tibble
#' 
#' @export
assign_domain_type <- function(conn1, emails){

  generic <- tbl(conn1, "domain_generic_domains") %>%
    collect() %>%
    mutate(email_type = "generic")

  domains <- urltools::domain(emails)
  emails_decomposed <- urltools::suffix_extract(domains)

  emails_decomposed %>% 
    left_join(generic, by = "host") %>% 
    mutate(
      email_type = case_when(
        is.na(email_type) & grepl("\\.edu|\\.etu|\\.ac", host) ~ "academic",
        TRUE ~ email_type
      ),
      email_type = ifelse(is.na(email_type), "business", email_type)
    ) %>% 
    pull(email_type)
}
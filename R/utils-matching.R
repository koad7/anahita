#' Matching Strengt 1
#' 
#' Domain and name matching
#' 
#' @param conn connection to the anahita DB
#' 
#' @examples 
#' \dontrun{
#' conn <- connect_anahita()
#' match_to_name_plus_domain(conn)
#' }
#' 
#' @export
match_to_name_plus_domain <- function(conn) {

  cli_process_start("Matching on name and domain")

  tbl_sf_id <- tbl(conn, "sf_id_table") %>%
    select(sfid, sf_name = name, sf_legal_name = legal_name, sf_url_clean = url_clean)

  tbl_cb_id <- tbl(conn, "cb_organizations") %>%
    select(uuid, cb_name = name, cb_legal_name = legal_name, cb_url_clean = domain)

  # matching
  matches <- tbl_sf_id %>%
    filter(!is.na(sf_url_clean)) %>%
    left_join(tbl_cb_id, by = c("sf_url_clean" = "cb_url_clean", "sf_name" = "cb_name")) %>%
    filter(!is.na(uuid)) %>%
    add_count(sfid, name = "n_sfid") %>%
    add_count(uuid, name = "n_uuid") %>%
    filter(n_sfid == 1 & n_uuid == 1) %>%
    select(-c(n_sfid, n_uuid)) %>% 
    mutate(cb_name = sf_name) %>% 
    collect()%>% 
    mutate(match_name = TRUE, match_domain = TRUE)

  cli_process_done(msg_done = "Matched {.val {nrow(matches)}} companies on name and domain")

  dbWriteTable(conn, "master", matches, overwrite = TRUE)
    
}

#' Matching Strengt 2
#' 
#' Domain matching
#' 
#' @param conn connection to the anahita DB
#' 
#' @examples 
#' \dontrun{
#' conn <- connect_anahita()
#' match_to_domain(conn)
#' }
#' 
#' @export
match_to_domain <- function(conn) {

  cli_process_start("Matching on domain")

  tbl_master <- tbl(conn, "master")

  tbl_sf_id <- tbl(conn, "sf_id_table") %>%
    select(sfid, sf_name = name, sf_legal_name = legal_name, sf_url_clean = url_clean) %>% 
    anti_join(select(tbl_master, "sfid"), by = "sfid")

  tbl_cb_id <- tbl(conn, "cb_organizations") %>%
    select(uuid, cb_name = name, cb_legal_name = legal_name, cb_url_clean = domain) %>% 
    anti_join(select(tbl_master, "uuid"), by = "uuid")

  matches <- tbl_sf_id %>%
    filter(!is.na(sf_url_clean)) %>%
    left_join(tbl_cb_id, by = c("sf_url_clean" = "cb_url_clean")) %>%
    filter(!is.na(uuid)) %>%
    add_count(sfid, name = "n_sfid") %>%
    add_count(uuid, name = "n_uuid") %>%
    filter(n_sfid == 1 & n_uuid == 1) %>%
    select(-c(n_sfid, n_uuid)) %>% 
    collect() %>% 
    mutate(match_domain = TRUE, match_name = FALSE)

  cli_process_done(msg_done = "Matched {.val {nrow(matches)}} companies on domain only")

  dbWriteTable(conn, "master", matches, append = TRUE)
}

#' Matching Strengt 3
#' 
#' Legal name matching
#' 
#' @param conn connection to the anahita DB
#' 
#' @examples 
#' \dontrun{
#' conn <- connect_anahita()
#' match_to_legal_name(conn)
#' }
#' 
#' @export
match_to_legal_name <- function(conn) {

  cli_process_start("Matching on company legal name")

  tbl_master <- tbl(conn, "master")

  tbl_sf_id <- tbl(conn, "sf_id_table") %>%
    select(sfid, sf_name = name, sf_legal_name = legal_name, sf_url_clean = url_clean) %>% 
    anti_join(select(tbl_master, "sfid"), by = "sfid")

  tbl_cb_id <- tbl(conn, "cb_organizations") %>%
    select(uuid, cb_name = name, cb_legal_name = legal_name) %>% 
    anti_join(select(tbl_master, "uuid"), by = "uuid")

  matches <- tbl_sf_id %>%
    filter(!is.na(sf_legal_name)) %>%
    left_join(tbl_cb_id, by = c("sf_legal_name" = "cb_legal_name")) %>%
    filter(!is.na(uuid)) %>%
    add_count(sfid, name = "n_sfid") %>%
    add_count(uuid, name = "n_uuid") %>%
    filter(n_sfid == 1 & n_uuid == 1) %>%
    select(-c(n_sfid, n_uuid)) %>% 
    collect() %>% 
    mutate(match_domain = TRUE, match_name = FALSE)

  cli_process_done(msg_done = "Matched {.val {nrow(matches)}} companies on legal name only")

  dbWriteTable(conn, "master", matches, append = TRUE)
}

#' Add missing companies
#' 
#' @description 
#' Add to the master table all companies that have
#' not been matched to CB.
#' 
#' @param conn connection to the anahita DB
#' 
#' @examples 
#' \dontrun{
#' conn <- connect_anahita()
#' add_missing_salesforce(conn)
#' }
#' 
#' @export
add_missing_salesforce <- function(conn){

  cli_process_start("Adding missing salesforce companies to master")

  tbl_master <- tbl(conn, "master") %>% 
    select("sfid", "uuid")

  missing <- tbl(conn, "sf_id_table") %>%
    select(sfid, sf_name = name, sf_legal_name = legal_name, sf_url_clean = url_clean) %>% 
    anti_join(tbl_master, by = "sfid") %>% 
    collect() %>% 
    mutate(match_domain = FALSE, match_name = FALSE)

  cli_process_done(msg_done = "Added {.val {nrow(missing)}} missing salesforce companies to master")

  dbWriteTable(conn, "master", missing, append = TRUE)
}

#' @noRd
#' @keywords internal
update_log <- function(conn){
  
  stats <- dbGetQuery(conn, "SELECT uuid IS NOT NULL as matched, match_name, match_domain, COUNT(1) FROM master GROUP BY matched, match_name, match_domain")

  log <- tibble(
    "last_updated" = lubridate::with_tz(Sys.time(), tzone = "UTC"),
    "total_tlos_sf" = sum(stats$count),
    "total_sf_cb_matches" = filter(stats, matched == TRUE) %>% pull(count) %>% sum(),
    "total_matches_name" = filter(stats, match_name == TRUE) %>% pull(count) %>% sum(),
    "total_matches_domain" = filter(stats, match_domain == TRUE) %>% pull(count) %>% sum(),
    "total_matches_name_domain" = filter(stats, match_domain == TRUE, match_name == TRUE) %>% pull(count) %>% sum()
  )

  dbWriteTable(conn, "master_updates", log, append = TRUE)

  cli_h2("Summary")
  cli_ul()
  cli_li("Total: {.val {sum(stats$count)}}")
  cli_li("Not matched: {.val {filter(stats, matched == FALSE) %>% pull(count) %>% sum()}}")
  cli_li("Matched: {.val {filter(stats, matched == TRUE) %>% pull(count) %>% sum()}}")

}

#' Table name
#' 
#' Convenience function to set table names
#' 
#' @description 
#' One wrapper function can be used to update the
#' knowledge tables for different environments.
#' 
#' @param envr environment (PROD, QA, STAGING)
#' 
#' @examples 
#' \dontrun{
#' knwl_set_table_name("QA")
#' }
#' 
#' @return a list of tibbles
#' 
#' @export
knwl_set_table_name <- function(envr) {
  
    if (envr == "PROD")
      my_tbl <- "knwl_feed_prod"
    else if (envr == "QA")
      my_tbl <- "knwl_feed_qa"
    else if (envr == "STAGING")
      my_tbl <- "knwl_feed_staging"
    else 
      stop("envr must match QA, STAGING or PROD")
    
    return(my_tbl)
}


#' Weekly moderation stats
#' 
#' @description 
#' Weekly moderation targets are based on achieving a number of
#' targets based on total volume of articles but also number of 
#' approved articles per topic and diversity of sources per topic
#' among the approved articles.
#' 
#' @param conn1 connection to a anahita DB
#' @param time1 is lower bound (default is Saturday as first day of the week)
#' @param time2 is upper bound (default is Saturday as last  day of the week)
#' 
#' @examples
#' \dontrun{
#' con_an <- connect_anahita()
#' dt <- get_moderation_summary(con_an)
#' }
#' 
#' @return a tibble
#' 
#' @importFrom lubridate ceiling_date floor_date
#' 
#' @export
knwl_moderation_summary <- function(
  conn1, 
  time1 = floor_date(Sys.Date(), "weeks", week_start = -1), 
  time2 = ceiling_date(Sys.Date(), "weeks", week_start = +6)) {

  sel_cols <- c(
    "a_time_analyzed", "a_articleid", "a_title", "a_link", 
    "a_summary", "a_approved", "a_authors", "a_topics", 
    "a_moderators", "s_id", "s_name", "s_url", "s_active", "s_auto_approved", "s_region", "s_language","s_content_type")

  dt <- tbl(conn1, "knwl_feed_prod") %>%
    select(sel_cols) %>%
    filter(a_time_analyzed >= time1 & a_time_analyzed < time2) %>%
    filter(s_active == "TRUE") %>%
    collect()

  if (nrow(dt) == 0) {
    time1 <- floor_date(Sys.Date(), "weeks", week_start = -1)-2
    dt <- tbl(conn1, "knwl_feed_prod") %>%
      select(sel_cols) %>%
      filter(a_time_analyzed >= time1 & a_time_analyzed < time2) %>%
      filter(s_active == "TRUE") %>%
      collect()
  }

  tms <- tbl(conn1, "sf_insight_area") %>%
    filter(status__c == "Active" & recordtype == "Insight Area") %>%
    select(sfid_ins = "id", name_ins = "name", title_ins = "publishable_title__c", type_ins = "type__c") %>%
    collect() %>%
    distinct(sfid_ins, .keep_all = TRUE)

  dt_unique_src_a <- dt %>%
    separate_rows(a_topics, sep = "; ") %>%
    filter(!is.na(a_topics)) %>%
    filter(a_approved == "TRUE") %>%
    group_by(a_topics) %>%
    summarise("unique_sources_a" = length(unique(s_name))) %>%
    ungroup() %>%
    replace_na(list(unique_sources_a = 0))
    
  dt_unique_src_i <- dt %>%
    separate_rows(a_topics, sep = "; ") %>%
    filter(!is.na(a_topics)) %>%
    filter(a_approved == "ignored") %>%
    group_by(a_topics) %>%
    summarise("unique_sources_i" = length(unique(s_name))) %>%
    ungroup() %>%
    replace_na(list(unique_sources_i = 0))

   dt_ins <- dt %>%
    separate_rows(a_topics, sep = "; ") %>%
    filter(!is.na(a_topics)) %>%
    group_by(a_topics) %>%
    summarise(
      "approved" = sum(a_approved == "TRUE"),
      "rejected" = sum(a_approved == "FALSE"),
      "ignored" = sum(a_approved == "ignored")
    ) %>%
    full_join(tms, by = c("a_topics" = "sfid_ins")) %>%
    filter(!is.na(name_ins)) %>%
    select(a_topics, name_ins, title_ins, type_ins, approved, rejected, ignored) %>%
    left_join(dt_unique_src_a, by = "a_topics") %>%
    left_join(dt_unique_src_i, by = "a_topics") %>%
    replace_na(list(
    approved = 0,
    rejected = 0,
    ignored = 0))
  
  dt_sum <- tibble(
    "approved"   = sum(dt$a_approved == "TRUE", na.rm = TRUE),
    "rejected"   = sum(dt$a_approved == "FALSE", na.rm = TRUE),
    "ignored"    = sum(dt$a_approved == "ignored", na.rm = TRUE),
    "total"      = nrow(dt),
    "vol_target" = ceiling(nrow(dt)*0.05),
    "top_n" = nrow(dt_ins[dt_ins$approved > 5,]),
    "top_target" = nrow(tms),
    "src_n" = nrow(dt_ins[dt_ins$unique_sources_a > 5,])
  ) %>%
  replace_na(list(
    approved = 0,
    rejected = 0,
    ignored = 0,
    total = 0,
    vol_target = 0,
    top_n = 0,
    top_target = 0,
    src_n = 0))

  return(list(dt_sum, dt_ins, dt))
  
}

#' Save TM network
#' 
#' @description
#' Salesforce not being a graph DB, it is complicated to
#' rebuild the full TM network in a friendly format. The
#' Toplink endpoint allows to do just that.
#'
#' @return a tibble
#'  
#' @export
knwl_toplink_network <- function() {

  df_net <- Rwefsigapi::tpk_get_network(3)
  colnames(df_net) <- gsub("\\.", "_", colnames(df_net))
  return(df_net)

}


#' Build outer ring article volume co-occurence
#' 
#' @param conn1 connection to anahita
#' @param time_since start time (default NULL)
#' @param time_to end time (default NULL)
#' 
#' @importFrom tidyr replace_na pivot_wider
#' @importFrom tibble add_column
#' 
#' @export
knwl_outer_ring_stats <- function(conn1, time_since = NULL, time_to = NULL) {
  
  tms_all <- tbl(conn1, "knwl_tm_network") %>%
    collect()
  
  tms <- tms_all %>% 
    select(sfid_ins, name_ins, title_ins, type_ins, sfid_top, name_top) %>%
    distinct(sfid_ins, sfid_top, .keep_all = TRUE)
  
  lst_ins <- tms %>%
    distinct(sfid_ins, .keep_all = TRUE)

  is_map_topic <- function(id1, vec_id2) {
    sapply(
      vec_id2,
      function(x) {
        tms %>%
          filter(sfid_ins == id1 & sfid_top == x[1]) %>%
          nrow(.)
      }
    )
  }

  get_connected_issues <- function(id1, vec_id2) {
    sapply(
      vec_id2,
      function(x) {
        vec_issues <- tms_all %>%
          filter(sfid_ins == id1 & sfid_top == x[1]) %>%
          select(name_iss) 
        paste0(vec_issues$name_iss, collapse = "; ")
      }
    )
  }
 
  calc_score <- function(df) {
    df_sub <- df %>%
      slice(1:20)

    sum(df_sub$map_topic)
  }

  tbl_art <- tbl(conn1, "knwl_feed_prod")

  if (!is.null(time_since))
    tbl_art <- tbl_art %>%
      filter(a_time >= time_since)

  if (!is.null(time_to))
    tbl_art <- tbl_art %>%
      filter(a_time < time_to)

  i <- 0

  cat("Outer ring calculations performed for ", nrow(lst_ins)," insight areas\n")

  do.call(
    bind_rows,
    apply(
      lst_ins,
      1,
      function(x) {
       
        cat(i+1, "/", nrow(lst_ins), " ---> ", as.character(x[2]),"\n")
        i <<- i+1
        id <- as.character(x[1])

        cols <- c(`FALSE` = NA_real_, `TRUE` = NA_real_, ignored = NA_real_)
  
        tbl_art %>%
          select(c("a_topics","a_approved")) %>%
          filter(grepl(id, a_topics)) %>%
          collect() %>%
          separate_rows(a_topics, sep = "; ") %>%
          filter(a_topics != id & a_topics %in% lst_ins$sfid_ins) %>%
          count(a_topics, a_approved) %>%
          pivot_wider(names_from = a_approved, values_from = n, values_fill = 0) %>%
          add_column(!!!cols[setdiff(names(cols), names(.))]) %>%
          replace_na(list(`FALSE` = 0, `TRUE` = 0, ignored = 0)) %>%
          left_join(select(lst_ins, c("sfid_ins", "name_ins", "title_ins", "type_ins")), by = c("a_topics" = "sfid_ins")) %>%
          rename(name_top = "name_ins", title_top = "title_ins", type_top = "type_ins", sfid_top = "a_topics") %>%
          rename(approved = "TRUE", rejected = "FALSE") %>%
          mutate(sfid_ins = id) %>%
          mutate(name_ins = as.character(x[2])) %>%
          mutate(title_ins = as.character(x[3])) %>%
          mutate(type_ins = as.character(x[4])) %>%
          select(sfid_ins, name_ins, title_ins, type_ins, sfid_top, name_top, title_top, type_top, approved, rejected, ignored) %>%
          mutate(connected_issues = get_connected_issues(id, sfid_top)) %>%
          mutate(map_topic = is_map_topic(id, sfid_top)) %>%
          arrange(desc(approved)) %>%
          mutate(score = calc_score(.)) %>%
          mutate(score_perc = round((score/20)*100,2))
  
      }
    ))
}
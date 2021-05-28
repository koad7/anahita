#' Update Summary Stats Table
#' 
#' @param conn connection to the anahita DB
#' @param time_since starting time (default is NULL)
#' @param time_to ending time (default is NULL)
#' 
#' @examples 
#' \dontrun{
#' conn <- connect_anahita()
#' update_si_stats(conn)
#' }
#' 
#' @return list of tibbles
#' 
#' @export
update_si_stats <- function(conn, time_since = NULL, time_to = NULL) {

  cat("Calculating SI stats\n")
  df <- get_si_user_stats(conn, id = NULL, time_since, time_to)
  cat("Collected", nrow(df), "rows\n")

  cat("Separate view for Paying DMS\n\n")
  stripe <- stripe_customers(conn) %>%
    distinct(id) %>%
    pull(id)

  df_dms <- df %>%
    filter(distinct_id %in% stripe)

  cat("Collected", nrow(df), "rows\n")

  cat("Saving to database\n")
  dbWriteTable(conn, "si_user_stats_summary", df, overwrite = TRUE)
  dbWriteTable(conn, "si_user_dms_stats_summary", df, overwrite = TRUE)
  cat("Saved to database\n")

  return(list(df, df_dms))

}

#' Update Summary Stats Table
#' 
#' @param conn connection to the anahita DB
#' @param nmax max numbe of cities per user (default is 5)
#' @param time_since starting time (default is NULL)
#' @param time_to ending time (default is NULL)
#' 
#' @examples 
#' \dontrun{
#' conn2 <- connect_anahita()
#' update_si_location(conn)
#' }
#' 
#' @export
update_si_location <- function(conn, nmax = 5, time_since = NULL, time_to = NULL) {

  cat("Calculating SI locations\n")
  df <- get_si_user_location(conn, nmax, id = NULL, time_since, time_to)
  cat("Collected", nrow(df), "rows\n")

  cat("Saving to database\n")
  dbWriteTable(conn, "si_user_stats_location", df, overwrite = TRUE)
  cat("Saved to database\n")

  return(df)

}

#' Update Summary Stats Table
#' 
#' @param conn connection to the anahita DB
#' 
#' @examples 
#' \dontrun{
#' conn <- connect_anahita()
#' update_si_browsing(conn)
#' }
#' 
#' @export
update_si_browsing <- function(conn) {

  max_time <- tbl(conn, "si_user_browsing") %>% 
    select(time) %>% 
    filter(time == max(time, na.rm = TRUE)) %>% 
    collect() %>%
    pull(time)

  time1 <- max_time[1]
  time2 <- Sys.Date()

  cat("Calculating SI Browsing\n")
  cat("Pulling data from", as.character(time1), "to", as.character(time2), "\n")
  df <- get_si_browsing(conn, id = NULL, time_since = time1, time_to = time2)
  cat("Collected", nrow(df), "rows\n")

  cat("Saving to database\n")
  dbWriteTable(conn, "si_user_browsing", df, append = TRUE)
  cat("Saved to database\n")

  return(df)

}

#' Update Top insights
#' 
#' @param conn connection to the anahita DB
#' @param nmax max numbe of insights per user (default is 5)
#' @param time_since starting time (default is NULL)
#' @param time_to ending time (default is NULL)
#' 
#' @examples 
#' \dontrun{
#' conn <- connect_anahita()
#' update_si_top_insights(conn)
#' }
#' 
#' @export
update_si_top_insights <- function(conn, nmax = 5, time_since = NULL, time_to = NULL) {

  cat("Calculating top insights\n")
  df <- get_si_user_top_insights(conn, nmax, id = NULL, time_since, time_to)
  cat("Collected", nrow(df), "rows\n")

  cat("Saving to database\n")
  dbWriteTable(conn, "si_user_stats_top_insights", df, overwrite = TRUE)
  cat("Saved to database\n")

  return(df)

}

#' Update Top Issues
#' 
#' @param conn connection to the anahita DB
#' @param nmax max numbe of insights per user (default is 5)
#' @param time_since starting time (default is NULL)
#' @param time_to ending time (default is NULL)
#' 
#' @examples 
#' \dontrun{
#' conn <- connect_anahita()
#' update_si_top_issues(conn)
#' }
#' 
#' @export
update_si_top_issues <- function(conn, nmax = 5, time_since = NULL, time_to = NULL) {

  cat("Calculating top issues\n")
  df <- get_si_user_top_issues(conn, nmax, id = NULL, time_since, time_to)
  cat("Collected", nrow(df), "rows\n")

  cat("Saving to database\n")
  dbWriteTable(conn, "si_user_stats_top_issues", df, overwrite = TRUE)
  cat("Saved to database\n")

  return(df)

}

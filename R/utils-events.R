#' Webinars attendance
#' 
#' @description 
#' Special webinars series were created as a a value proposition
#' to digital members and New Champions. Tracking engagement was
#' important but these webinar series did not last for long.
#'
#' @param conn1 connection to the DW
#' @param emails vector of emails for filtering
#' 
#' @examples
#' \dontrun{
#' con_dw <- connect_dw()
#' events_webinar_attendance(con_dw)
#' }
#' 
#' @return a tibble
#' 
#' @export
events_webinar_attendance <- function(conn1, emails = NULL) {

  
  if (!is.null(emails)) {
    tbl(conn1, in_schema("ods", "zoom_report_webinars_participants_v")) %>% 
      left_join(tbl(conn1, in_schema("ods", "zoom_report_webinars_v")), by = "webinar_id") %>%
      filter(user_email %in% emails) %>%
      collect()
  } else {
    tbl(conn1, in_schema("ods", "zoom_report_webinars_participants_v")) %>% 
      left_join(tbl(conn1, in_schema("ods", "zoom_report_webinars_v")), by = "webinar_id") %>%
      collect()
  }

}


#' Events attendance
#' 
#' @description 
#' Collecting event registration and attendance informations for 
#' all digital members (paying and institutional)
#'
#' @param conn1 connection to the DW
#' @param sel_ids vector of ids
#' @param max_date when to start pulling
#' @param sessions session ids
#' 
#' @examples
#' \dontrun{
#' con_dw <- connect_dw()
#' events_attendance(con_dw)
#' }
#' 
#' @return a tibble
#' 
#' @export
events_attendance <- function(conn1, sel_ids = NULL, max_date = NULL, sessions = NULL) {

  sel_cols <- c(
    "account_sfdc_id", "account_fullname", "account_pem", "admission_category_name", 
    "account_group", "attendance_login_count", "attendance_rate_grouping", "attendance_rate", 
    "attendance_time_minutes", "attendance_session_time_minutes", "attendance_user_email", 
    "attendance_user_id", "event_sfdc_id", "event_publishable_name", "event_level_type", 
    "event_series_name", "event_start_date", "event_end_date", 
    "opportunity_stage", "organization_sfdc_id", "organization_name", "organization_pem",
    "organization_primary_community", "organization_subtype",	"organization_type",	
    "participation_status", "position_country", "position_name", "position_parent_region",
    "position_region", "registration_status", "role_record_type", "role_status", 
    "role_type", "session_sfdc_id", "session_name", "session_owner", "session_short_name",
    "session_start_date_utc", "session_end_date_utc", "session_duration", "si_subscription_type", 
    "session_format", "signup_attendance_status", "top_forum_engagement_flag", 
    "top_level_organization_name", "virtuals_session_type"
  )

  dms_events <- tbl(conn1, in_schema("enorm", "wef_event_attendance_v"))
  
  if (!is.null(max_date)) {
    cat("---> latest session date:", as.character(max_date), "\n")
    dms_events <- dms_events %>%
      filter(session_start_date_utc >= max_date)
  }

  if (!is.null(sessions)) {
    dms_events <- dms_events %>%
      filter(!session_sfdc_id %in% sessions)
  }

  if (!is.null(sel_ids)) {
    dms_events <- dms_events %>%
      filter(account_sfdc_id %in% sel_ids)
  }
  
  dms_events %>%
    select(all_of(sel_cols)) %>%
    collect()

}
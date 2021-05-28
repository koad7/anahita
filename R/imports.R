#' @importFrom dplyr add_count all_of arrange as_tibble bind_rows collect desc distinct
#' @importFrom dplyr filter left_join mutate pull rename rename_all rowwise select tbl ungroup
#' @importFrom dbplyr in_schema
#' @importFrom RPostgres Postgres
#' @importFrom DBI dbConnect dbDisconnect dbWriteTable
#' @importFrom lubridate add_with_rollback
#' @importFrom stringr str_sub
#' @importFrom tidyr pivot_longer
#' @importFrom cli cli_h1 cli_alert_danger cli_process_start cli_process_done
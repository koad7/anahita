#' Update Agenda Blogs
#' 
#' @description 
#' Since the endpoints provide a pagination approach but
#' results can't ne queries by time, the updates pulls a 
#' number of pages and articles per pages and compares them
#' with the online table in anahita.
#' 
#' @param conn connection to the data anahita DB
#' @param pages how far back you want to search for new articles (default is 5)
#' @param n how many articles per pages (default is 25)
#' @param verbose verbose message or not (default = TRUE)
#' 
#' @examples 
#' \dontrun{
#' conn <- connect_anahita()
#' update_agenda_blogs(conn)
#' }
#' 
#' @export
update_agenda_blogs <- function(conn, pages = 5, n = 25, verbose = TRUE) {

  cli_h1("Updating Forum Agenda Blogs")

  results <- get_forum_agenda_blogs(pages, n, verbose)
  
  agenda_blogs <- tbl(conn, "agenda_articles") %>%
    select(article_id) %>%
    collect() %>%
    distinct(article_id)

  new_agenda_blogs <- results %>%
    anti_join(agenda_blogs, by = "article_id")

  cat(crayon::green("Number of new agenda blogs found:"), crayon::blue(nrow(new_agenda_blogs)), "\n")
  
  if (nrow(new_agenda_blogs) > 0) {
    cli_process_start("Saving to remote table")
    dbWriteTable(conn, "agenda_articles", new_agenda_blogs, append = TRUE)
    cli_process_done(msg_done = "Saved to anahita DB")
  }

  return(new_agenda_blogs)

}

#' Dump all Agenda Blogs
#' 
#' @description 
#' Run the agenda blog query function accross all pages and
#' overwrite the main table in anahita.
#' 
#' @param conn connection to the data anahita DB
#' @param pages total pages to retrieve (default is NULL to retrieve all)
#' @param n how many articles per pages (default is 25)
#' @param verbose verbose message or not (default = TRUE)
#' 
#' @examples 
#' \dontrun{
#' conn <- connect_anahita()
#' dump_all_agenda_blogs(conn)
#' }
#' 
#' @export
dump_all_agenda_blogs <- function(conn, pages = NULL, n = 25, verbose = TRUE) {

  cli_h1("Dumping Forum Agenda Blogs")

  results <- get_forum_agenda_blogs(pages, n, verbose)
  
  cli_process_start("Saving to remote table")
  dbWriteTable(conn, "agenda_articles", results, overwrite = TRUE)
  cli_process_done(msg_done = "Saved to anahita DB")

  return(results)
}
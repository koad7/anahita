#' Forum Agenda articles
#' 
#' @description 
#' It is of strategic interest for communities to
#' closely monitor their members contributions to various
#' editprial products of the Forum, such as Agenda blogs or 
#' reports. We maintain a table with all those publications
#' with others with accounts in SF.
#'
#' @param pages how many pages to paginate through (default is 1)
#' @param n how many articles per pages (default is 25)
#' @param verbose verbose message or not (default = TRUE)
#' 
#' @examples
#' df <- get_forum_agenda_blogs(5, 10)
#' 
#' @return a tibble
#' 
#' @export
get_forum_agenda_blogs <- function(pages = 1, n = 25, verbose = TRUE) {

  data <- api_call_agenda_articles(1, n)
  total_pages <- as.integer(data$meta$pagination$total_pages)
  total_counts <- as.integer(data$meta$pagination$total_count)
  results <- format_agenda_blogs(data$data)
  
  if (!is.null(pages)) {
    if (pages < total_pages)
      total_pages <- pages
  }

  if (verbose) {
    cat(green("Total number of pages to retrieve    :"), blue(total_pages), "\n")
    cat(green("Total number of articles to retrieve :"), blue(total_counts), "\n")
  }

  if (total_pages > 1) {
    for (ipage in 2:total_pages) {

    data <- api_call_agenda_articles(ipage, n)
    
    if (verbose) {
      cat(
        green("Retrieving page"), 
        red(sprintf("%05d", ipage)), 
        green("/", total_pages, " ---> found"), 
        red(sprintf("%03d", length(data$data$id))), 
        green("articles\n"))
    }
  
    if (length(data$data$id) > 0) {
      results <- bind_rows(results, format_agenda_blogs(data$data))
    }

    }
  }

  return(results)
  
}

#' Endpoint call for Agend
#'
#' @param page page number
#' @param n how many articles to retrieve
#' 
#' @examples
#' list <- api_call_agenda_articles(1, 20)
#' 
#' @return a list
#' 
#' @export
api_call_agenda_articles <- function(page = 1, n = 25) {

  base_url <- "https://api.weforum.org/v1/articles"
  uri <- paste0(base_url, "?page%5Bnumber%5D=", page, "&page%5Bsize%5D=", n)
  fromJSON(uri)

}

#' Forum Agenda articles list to df
#' 
#' @param list a list object to parse (see 'api_call_agenda_articles')
#' 
#' @return a `tibble`
#' 
#' @export
format_agenda_blogs <- function(list) {

  df_article <- tibble(
    article_id = as.character(),
    article_title = as.character(),
    article_slug = as.character(),
    article_published_at = as.character(),
    article_updated_at = as.character(),
    article_api_endpoint = as.character(),
    article_api_toplink_endpoint = as.character(),
    article_website_endpoint = as.character(),
    sf_id = as.character(),
    first_name = as.character(),
    surname = as.character(),
    biography = as.character(),
    position = as.character(),
    organisation = as.character(),
    twitter_username = as.character(),
    facebook_page = as.character(),
    linkedin_page = as.character()
    # topic_id = as.character(),
    # topic_title = as.character(),
    # topic_end_point = as.character(),
    # topic_type = as.character()
  )

  x <- list$attributes
  y <- list$attributes$authors
  z <- list$attributes$featured_or_first_topic

  for (i in 1:length(list$id)) {

    df_article <- df_article %>%
      bind_rows(
        tibble::tibble(
          article_id = ifelse(is.null(list$id[i]), NA, list$id[i]),
          article_title = ifelse(is.null(x[["title"]][i]), NA, x[["title"]][i]),
          article_slug = ifelse(is.null(x[["slug"]][i]), NA, x[["slug"]][i]),
          article_published_at = ifelse(is.null(x[["published_at"]][i]), NA, x[["published_at"]][i]),
          article_updated_at = ifelse(is.null(x[["updated_at"]][i]), NA, x[["updated_at"]][i]),
          article_api_endpoint = ifelse(is.null(x[["api_endpoint"]][i]), NA, x[["api_endpoint"]][i]),
          article_api_toplink_endpoint = ifelse(is.null(x[["api_toplink_endpoint"]][i]), NA, x[["api_toplink_endpoint"]][i]),
          article_website_endpoint = ifelse(is.null(x[["website_endpoint"]][i]), NA, x[["website_endpoint"]][i]),
          sf_id = ifelse(is.null(y[[i]][["sf_id"]]), NA, y[[i]][["sf_id"]]),
          first_name = ifelse(is.null(y[[i]][["first_name"]]), NA, y[[i]][["first_name"]]),
          surname = ifelse(is.null(y[[i]][["surname"]]), NA, y[[i]][["surname"]]),
          biography = ifelse(is.null(y[[i]][["biography"]]), NA, y[[i]][["biography"]]),
          position = ifelse(is.null(y[[i]][["position"]]), NA, y[[i]][["position"]]),
          organisation = ifelse(is.null(y[[i]][["organisation"]]), NA, y[[i]][["organisation"]]),
          twitter_username = ifelse(is.null(y[[i]][["twitter_username"]]), NA, y[[i]][["twitter_username"]]),
          facebook_page = ifelse(is.null(y[[i]][["facebook_page"]]), NA, y[[i]][["facebook_page"]]),
          linkedin_page = ifelse(is.null(y[[i]][["linkedin_page"]]), NA, y[[i]][["linkedin_page"]])
          #topic_id = ifelse(ifelse(is.null(z[["sf_id"]][i]), NA, z[["sf_id"]][i]),
          #topic_title = ifelse(is.null(z[["title"]][i]), NA, z[["title"]][i]),
          #topic_end_point = ifelse(is.null(z[["api_endpoint"]][i]), NA, z[["api_endpoint"]][i]),
          #topic_type = ifelse(is.null(z[["type"]][i]), NA, z[["type"]][i])
        )
      )
  }

  df_article %>%
    rowwise() %>%
    mutate(article_title = trimws(article_title, which = "both")) %>%
    mutate(article_slug = trimws(article_slug, which = "both")) %>%
    mutate(first_name = trimws(first_name, which = "both")) %>%
    mutate(surname = trimws(surname, which = "both")) %>%
    mutate(name = paste(first_name, surname)) %>%
    mutate(biography = trimws(biography, which = "both")) %>%
    mutate(position = trimws(position, which = "both")) %>%
    mutate(organisation = trimws(organisation, which = "both")) %>%
    #mutate(topic_title = trimws(topic_title, which = "both")) %>%
    mutate(article_published_at = as.Date(strsplit(article_published_at, "T")[[1]][1])) %>%
    mutate(article_updated_at = as.Date(strsplit(article_updated_at, "T")[[1]][1])) %>%
    ungroup() %>%
    mutate_if(is.character, list(~na_if(., "")))

}

% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/utils-forum-agenda.R
\name{api_call_agenda_articles}
\alias{api_call_agenda_articles}
\title{Endpoint call for Agend}
\usage{
api_call_agenda_articles(page = 1, n = 25)
}
\arguments{
\item{page}{page number}

\item{n}{how many articles to retrieve}
}
\value{
a list
}
\description{
Endpoint call for Agend
}
\examples{
list <- api_call_agenda_articles(1, 20)

}

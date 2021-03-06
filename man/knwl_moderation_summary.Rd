% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/utils-knowledge.R
\name{knwl_moderation_summary}
\alias{knwl_moderation_summary}
\title{Weekly moderation stats}
\usage{
knwl_moderation_summary(
  conn1,
  time1 = floor_date(Sys.Date(), "weeks", week_start = -1),
  time2 = ceiling_date(Sys.Date(), "weeks", week_start = +6)
)
}
\arguments{
\item{conn1}{connection to a anahita DB}

\item{time1}{is lower bound (default is Saturday as first day of the week)}

\item{time2}{is upper bound (default is Saturday as last  day of the week)}
}
\value{
a tibble
}
\description{
Weekly moderation targets are based on achieving a number of
targets based on total volume of articles but also number of
approved articles per topic and diversity of sources per topic
among the approved articles.
}
\examples{
\dontrun{
con_an <- connect_anahita()
dt <- get_moderation_summary(con_an)
}

}

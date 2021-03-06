% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/update-knowledge.R
\name{knwl_update_weekly_moderation_targets}
\alias{knwl_update_weekly_moderation_targets}
\title{Weekly moderation stats}
\usage{
knwl_update_weekly_moderation_targets(
  dev = config_get(deploy_status),
  unit_time = "week",
  unit_back = 56,
  week_starts = -1
)
}
\arguments{
\item{dev}{deployment status read from config (dev will not right into DBs)}

\item{unit_time}{set to week (can be day, month, year)}

\item{unit_back}{set 56 weeks back}

\item{week_starts}{set -1 (starts Saturday)}
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
dt <- knwl_update_weekly_moderation_targets(con_an)
}

}

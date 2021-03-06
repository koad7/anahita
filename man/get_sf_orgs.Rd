% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/utils-sf.R
\name{get_sf_orgs}
\alias{get_sf_orgs}
\title{Retrieve SF company}
\usage{
get_sf_orgs(conn)
}
\arguments{
\item{conn}{connection to the data warehouse}
}
\value{
a tibble
}
\description{
In the current anahita parading, Salesforce TLOs are the reference
for all matching procedures to other source of data like CrunchBase. We
however also pull inactive company accounts to track historical records
when looking at potential company engagement.
}
\examples{
\dontrun{
conn <- connect_dw()
get_sf_orgs(conn)
}

}

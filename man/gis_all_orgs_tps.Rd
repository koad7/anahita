% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/utils-gis.R
\name{gis_all_orgs_tps}
\alias{gis_all_orgs_tps}
\title{Retrieve all TP companies}
\usage{
gis_all_orgs_tps(conn)
}
\arguments{
\item{conn}{connection to the data warehouse}
}
\value{
a tibble
}
\description{
Note that TPs don't have a contract so we actually
wrap this functoon around the community based
query \code{get_tlo_communities}
}
\details{
Retrieve all TP companies based on
contract information.
}
\examples{
\dontrun{
conn <- connect_dw()
gis_all_orgs_tps(conn)
}

}

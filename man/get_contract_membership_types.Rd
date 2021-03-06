% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/utils.R
\name{get_contract_membership_types}
\alias{get_contract_membership_types}
\title{Get membership types}
\usage{
get_contract_membership_types(conn)
}
\arguments{
\item{conn}{connection to the data warehouse}
}
\value{
a tibble
}
\description{
Get list of all contract based membership types
}
\examples{
\dontrun{
conn <- connect_dw()
get_contract_membership_types(conn, dt$org_id)
}

}

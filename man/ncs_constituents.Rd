% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/utils-ncs.R
\name{ncs_constituents}
\alias{ncs_constituents}
\title{NC Constituent accounts}
\usage{
ncs_constituents(conn, org_ids, people_ids = NULL)
}
\arguments{
\item{conn}{connection to the data warehouse}

\item{org_ids}{list of active organisation ids}

\item{people_ids}{list of people ids to exclude (default is NULL)}
}
\value{
a tibble
}
\description{
We find that not all person accounts under a NC comnpany
are part of the New Champions people community. You need to
exclude people in \code{ncs_members} to find the latter.
}
\details{
Identify consituents within New Champions companies as
a primary organization.
}
\examples{
\dontrun{
conn <- connect_dw()
dt <- ncs_all_orgs(conn)
ncs_constituents(conn, "a0e0X00000hzdENQAY")
ncs_constituents(conn, "a0e0X00000hzdENQAY", dt$org_id)
list_members <- ncs_members(conn, "a0e0X00000hzdENQAY", dt$org_id)
ncs_constituents(conn, dt$org_id, list_members$id)
}

}

% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/utils-ncs.R
\name{ncs_wrong_memberships}
\alias{ncs_wrong_memberships}
\title{Inactivate Memberships}
\usage{
ncs_wrong_memberships(conn, membership_ids, org_ids)
}
\arguments{
\item{conn}{connection to the data warehouse}

\item{membership_ids}{list of membership ids}

\item{org_ids}{list of active organisation ids}
}
\value{
a tibble
}
\description{
Because there is no automation to remove people from
the community when a contract is terminated, we need to
identify these on a regular basis.
}
\details{
Identify memberships that should ne inactivated.
}
\examples{
\dontrun{
conn <- connect_dw()
dt <- ncs_all_orgs(conn)
ncs_wrong_memberships(conn, "a0e0X00000hzdENQAY")
ncs_wrong_memberships(conn, "a0e0X00000hzdENQAY", dt$org_id)
}

}

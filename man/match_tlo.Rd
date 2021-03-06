% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/utils-whitelisting.R
\name{match_tlo}
\alias{match_tlo}
\title{Match tlo}
\usage{
match_tlo(conn, people_tbl)
}
\arguments{
\item{conn}{connection to the data warehouse}

\item{people_tbl}{tibble of people accounts (see \code{get_ptms} or \code{get_dms})}
}
\value{
a tibble
}
\description{
Match the tlo based on the whitelisting information
available.
}
\examples{
\dontrun{
conn <- connect_dw()
dms <- get_dms(conn)
match_tlo(conn, dms)
}

}

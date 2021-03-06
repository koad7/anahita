% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/update-ncs.R
\name{update_ncs_tables}
\alias{update_ncs_tables}
\title{Update all NCs tables}
\usage{
update_ncs_tables(conn1, conn2, id = "a0e0X00000hzdENQAY")
}
\arguments{
\item{conn1}{connection to the data warehouse}

\item{conn2}{connection to the anahita DB}

\item{id}{NC member community SF id (set to a0e0X00000hzdENQAY)}
}
\description{
Update all NCs tables
}
\examples{
\dontrun{
conn1 <- connect_dw()
conn2 <- connect_anahita()
update_ncs_tables(conn1, conn2)
}

}

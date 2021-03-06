% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/update-mixpanel.R
\name{initialize_all_views}
\alias{initialize_all_views}
\title{Initialize views}
\usage{
initialize_all_views(f1 = NULL, f2 = NULL, dev = config_get(deploy_status))
}
\arguments{
\item{f1}{serendipity event dump file location (if not provided queries DB)}

\item{f2}{toplink event dump file location (if not provided queries DB)}

\item{dev}{deployment status (dev does not push tables)}
}
\value{
a tibble
}
\description{
Using a massive dump of the mixpanel events table, one
creates clean views of relevant tables from scratch.
}
\examples{
\dontrun{
conn1 <- connect_dw()
conn2 <- connect_anahita()
update_si_stats(conn1, conn2)
}

}

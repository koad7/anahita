% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/update-stripe.R
\name{update_stripe_table}
\alias{update_stripe_table}
\title{Update stripe customers}
\usage{
update_stripe_table(
  dev = config_get(deploy_status),
  stripe_key = config_get(stripe)
)
}
\arguments{
\item{dev}{deployment status read from config (dev will not right into DBs)}

\item{stripe_key}{stripe api key}
}
\value{
tibble
}
\description{
This daily automation crawls the data from stripe and
matches customers with various sources of data such as
salesforce and crunchbase.
}
\examples{
\dontrun{
update_stripe_table()
}

}

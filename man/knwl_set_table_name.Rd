% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/utils-knowledge.R
\name{knwl_set_table_name}
\alias{knwl_set_table_name}
\title{Table name}
\usage{
knwl_set_table_name(envr)
}
\arguments{
\item{envr}{environment (PROD, QA, STAGING)}
}
\value{
a list of tibbles
}
\description{
One wrapper function can be used to update the
knowledge tables for different environments.
}
\details{
Convenience function to set table names
}
\examples{
\dontrun{
knwl_set_table_name("QA")
}

}

% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/utils-sf.R
\name{get_sf_topics}
\alias{get_sf_topics}
\title{Retrieve SF topics}
\usage{
get_sf_topics(conn)
}
\arguments{
\item{conn}{connection to the data warehouse}
}
\value{
a tibble
}
\description{
Retrieve list of all insight areas
}
\examples{
\dontrun{
conn <- connect_dw()
get_sf_topics(conn)
}

}

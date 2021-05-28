
<!-- README.md is generated from README.Rmd. Please edit that file -->

<!-- badges: start -->

<!-- badges: end -->

# anahita

The **anahita** package provides a suit of methods to faciliate the
access and management of data related to Strategic Intelligence
communities: New Champions, Global Innovators, Digital Members, as well
our pool of data regarding company potentials (Nr2, CrunchBase, D\&B
Hoovers).

The methods are used to maintained the various tables available in the
anahita Postgres database running on our AWS instance.

The goal is for us to use the anahita methods accross all our products
to guarantee consistency and reproducibility.

## Installation

The package has a number of dependencies you must install. In
particular, you should install the
[Nr2](https://github.com/forum-intelligence/nr2) package available on
the Strategic Intelligence GitHub account:

    remotes::install_github("https://github.com/forum-intelligence/nr2")
    remotes::install_github("https://github.com/forum-intelligence/anahita")

# Getting Started

## Setting Credentials

To get started, you’ll need to setup a number of authentication
credentials to access the various remote and internal databases. You can
run the method `anahita::create_config()` and edit the file with the
appropriate credentials.

Note that for Nr2, the authentication requires to retrieve a bearer
token that remains valid for 7 days. This is documented in the
corresponding [Nr2](https://github.com/forum-intelligence/nr2) package.

## Opening Connections

The package provides some utility functions to faciliate the connections
to various endpoints and tables. All functions however take the
connection as an argument so you are free to set your connections in
your preferred way. The package uses the `DBI::dbConnect` method.
Available connections are presented below and assume you have properly
set-up your credentials in the configuration file.

    anahita::connect_dw()
    anahita::connect_anahita()
    anahita::connect_dm()

# Company related ETL

Every day, we run a procedure that extract all company records from
Salesforce and from CrunchBase and perform a number of cleaning and
matching operations.

# Community related ETL

For practicality, we index every method with an prefix for the various
communities, namely ncs, gis, dms, etc. Some community specific methods
are actually wrappers over more generic methods that can be used. For
example, `ncs_invoices` calls the method `get_tlo_invoices`,
`gis_all_orgs_gis` calls `get_tlo_contracts`.

## Contract based communities

Technically, there are different ways to retrieve the organisations
belonging to different communities. The simplest method is to access all
organisations in the Salesforce contract based communities. These
communities are automatically managed by contract information. The
alternative is to query directly the contracts. We favor the latter for
many methods. However, a community like the Technology Pioneers, which
is not contract contract based, needs to be queried through the
community membership.

## New Champions

The main function that is called by for scheduled tasks is
`update_ncs_tables`. It relies on a number of utility functions that can
be very useful in many contexts.

A couple of examples are shown below.

``` r
conn <- connect_dw()
# Retrieve all NCs organisations with active contracts
ncs_orgs <- ncs_all_orgs(conn)
# Retrieve all NCs organisations by memebrship
ncs_orgs <- ncs_all_orgs_by_membership(conn)
# Retrieve NCs members of the New Champions community
ncs_memb <- ncs_members(conn, ncs_orgs$org_id)
# Retrieve GI companies (includes TPs)
gis_orgs <- gis_all_orgs()
# Retrieve TFE and Operational Contacts for GIs
gis_contacts(conn, gis_orgs$org_id)
```

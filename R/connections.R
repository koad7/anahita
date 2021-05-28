
#' Connect to anahita
#' @export
connect_anahita <- function(){

  user <- config_get(aurora_user)
  pw <- config_get(aurora_pass)

  dbConnect(
    drv = Postgres(),
    dbname = "anahita",
    host = "anahita.cluster-c6r5oht7mo1e.eu-west-1.rds.amazonaws.com",
    port = 5432,
    user = user,
    password = pw
  )

}

#' Connect to anahita
#' @export
connect_anahita_aur <- function(){

  user <- config_get(aurora_user)
  pw <- config_get(aurora_pass)

  dbConnect(
    drv = RPostgreSQL::PostgreSQL(),
    dbname = "anahita",
    host = "anahita.cluster-c6r5oht7mo1e.eu-west-1.rds.amazonaws.com",
    port = 5432,
    user = user,
    password = pw
  )

}


#' Connect to anahita
#' @export
connect_anahita_old <- function(){
  user <- config_get(user_anahita)
  pw <- config_get(password_anahita)

  dbConnect(
    drv = Postgres(),
    dbname = "anahita",
    host = "192.168.166.56",
    port = 5432,
    user = user,
    password = pw
  )

}

#' Connect to data warehouse
#' @export
connect_dw <- function(){
  user <- config_get(user_dw)
  pw <- config_get(password_dw)

  dbConnect(
    drv = Postgres(),
    dbname = "dw",
    host = "dw.weforum.local",
    port = 5439,
    user = user,
    password = pw,
    sslmode = "require"
  )
}

#' Connect to data dm db
#' @export
connect_dm <- function(){
  user <- config_get(user)
  pw <- config_get(password)

  dbConnect(
    drv = Postgres(),
    dbname = "digital-members",
    host = "192.168.166.56",
    port = 5432,
    user = user,
    password = pw
  )
}

#' Connect to data mixpanel db
#' @export
connect_mixpanel <- function(){
  user <- config_get(user)
  pw <- config_get(password)

  dbConnect(
    drv = Postgres(),
    dbname = "mixpanel",
    host = "192.168.166.56",
    port = 5432,
    user = user,
    password = pw
  )
}

#' Connect to knowledge feed
#' @export
connect_knwl <- function(){
  user <- config_get(user)
  pw <- config_get(password)

  dbConnect(
    drv = Postgres(),
    dbname = "knowledgefeed",
    host = "192.168.166.56",
    port = 5432,
    user = user,
    password = pw
  )
}

#' Connect to aurora AWS db
#' @export
connect_aurora <- function() {

  user <- config_get(aurora_user)
  pw <- config_get(aurora_pass)

  dbConnect(
    drv = Postgres(),
    dbname = "anahita",
    host = "anahita.cluster-c6r5oht7mo1e.eu-west-1.rds.amazonaws.com",
    port = 5432,
    user = user,
    password = pw
  )

}

#' Connect to Bs4Dash
#' @export
connect_bs <- function() {

  user <- config_get(user)
  pw <- config_get(password)
  
  dbConnect(
    drv = Postgres(),
    dbname = "bs-dash",
    host = "192.168.166.56",
    port = 5432, 
    user = user,
    password = pw
  )
}

#' disconnect
#' @param con connection to close 
#' @export
disconnect <- function(con){
  dbDisconnect(con)
}

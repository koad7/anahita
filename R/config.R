CONF <- "_config.yml"

#' Config
#' 
#' Create default configuration file
#' 
#' @importFrom assertthat assert_that
#' @importFrom yaml write_yaml read_yaml
#' 
#' @export
config_create <- function(){
  conf <- list(
    user_anahita = "initials",
    password_anahita = "password",
    user_dw = "username",
    password_dw = "password",
    cb_token = "token",
    nr2_email = "nr2_email",
    nr2_secret = "nr2_secret",
    aurora_user = "aurora_user",
    aurora_password = 'aurora_pass',
    deploy_status = 'deploy_status',
    stripe = 'stripe_key',
    mp_si = 'mp_si',
    mp_dm = 'mp_dm',
    LIMIT_ARTICLES = 50,
    LIMIT_SOURCES = 20,
    api_key_neo4j = "api_key_neo4j",
    api_key_neo4j_qa = "api_key_neo4j_qa",
    api_key_neo4j_stag = "api_key_neo4j_stag"
  )

  write_yaml(conf, CONF)
}

#' @noRd
#' @keywords internal
config_read <- function(){
  assert_that(has_config())
  read_yaml(CONF)
}

#' Get Config Parameter
#' 
#' @description 
#' Special method to retrieve configuration parameter
#' from local hidden config file.
#' 
#' @param var config variable
#' 
#' @export
config_get <- function(var){
  conf <- config_read()
  var_deparsed <- deparse(substitute(var))
  conf[[var_deparsed]]
}
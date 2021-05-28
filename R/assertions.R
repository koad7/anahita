has_config <- function() {
  file.exists(CONF)
}

assertthat::on_failure(has_config) <- function(call, env) {
  "Missing configuration file"
}
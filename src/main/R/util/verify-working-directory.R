# Ensure we are starting in the base directory
verify_wd <- function(path, expected) {
  path <- strsplit(path, "/")[[1]]
  actual <- tail(path, n=1)
  stopifnot(actual == expected)
}

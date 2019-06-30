################################################################################################
# Copyright (c) 2019 Brian Detweiler
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
# 
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.
#
# About:
# Initializes the NRD databases in MonetDB.
################################################################################################

# install.packages( c("MonetDB.R", "MonetDBLite") , repos=c("http://dev.monetdb.org/Assets/R/", "http://cran.rstudio.com/"))
library('MonetDBLite')
library('DBI')

source('src/main/R/util/verify-working-directory.R')

tryCatch({
  path <- getwd()
  verify_wd(path, "c-diff-and-renal-failure")
}, warning = function(e) {
  stop("Ensure working directory is set to `c-diff-and-renal-failure`.")
}, error = function(e) {
  stop("Ensure working directory is set to `c-diff-and-renal-failure`.")
})

# setwd('/home/bdetweiler/src/Data_Science/stat-8960-capstone-project')

# MonetDB connection to a permanent file
# Call the below line if you get an error about connecting
# MonetDBLite::monetdblite_shutdown()

con <- DBI::dbConnect(MonetDBLite::MonetDBLite(), "data/nrd_db")

################################################################################################
####                              CREATE HOSPITAL TABLE                                     ####
################################################################################################
create.hospital.table.sql <- readLines('src/main/resources/sql/nrd-hospital-create-table.sql')
create.hospital.table.sql <- paste(create.hospital.table.sql, collapse = "")

DBI::dbSendQuery(con, create.hospital.table.sql)

# DBI::dbSendQuery(con, "DROP TABLE nrd")
# DBI::dbSendQuery(con, "DROP TABLE nrd_hospital")
# DBI::dbGetQuery(con, "SELECT COUNT(key_nrd) as count FROM nrd")
################################################################################################
####                              CREATE NRD TABLE                                          ####
################################################################################################
create.nrd.table.sql <- readLines('src/main/resources/sql/nrd-create-table.sql')
create.nrd.table.sql <- paste(create.nrd.table.sql, collapse = "")

DBI::dbSendQuery(con, create.nrd.table.sql)


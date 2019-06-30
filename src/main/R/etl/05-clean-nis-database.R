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
# Modify the columns that were initialized as VARCHARs to accommodate for missing data codes
# into their ideal data types (typically INTEGER or DOUBLE) so we can offload calculations 
# to the database. For all integer and double columns, replace non-numerics with null.
# We'll use the technique specified here: 
# https://stackoverflow.com/questions/27770370/alter-the-data-type-of-a-column-in-monetdb
################################################################################################

# install.packages( c("MonetDB.R", "MonetDBLite") , repos=c("http://dev.monetdb.org/Assets/R/", "http://cran.rstudio.com/"))
library('MonetDBLite')
library('tidyverse')
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

# Get a connection to the database
# MonetDBLite::monetdblite_shutdown()
con <- DBI::dbConnect(MonetDBLite::MonetDBLite(), "data/nis_db")


column.names <- readLines('src/main/resources/metadata/convert-varchar-columns-to-integers.txt')
column.names
column.name <- "died"
for (column.name in column.names) {
  q <- DBI::dbGetQuery(con, paste0("SELECT DISTINCT(", column.name, ") as d FROM nis"))

  replace <- q %>% 
    filter(!is.na(d)) %>%
    filter(is.na(as.integer(d))) %>%
    pull(d)
  
  if (length(replace) == 0) {
    print(paste0("Nothing to update for ", column.name))
    next
  }
  # Enclose in single quotes for query
  replace <- paste0("'", replace, "'")

  replace <- paste(replace, collapse=",")
  update.query <- paste0("UPDATE nis SET ", column.name, " = null WHERE ", column.name, " in (", replace, ")")
  print(update.query)
  
  DBI::dbSendQuery(con, update.query)
} 

# For all double columns, replace non-doubles with null
column.names <- readLines('src/main/resources/metadata/convert-varchar-columns-to-doubles.txt')
for (column.name in column.names) {
  q <- DBI::dbGetQuery(con, paste0("SELECT DISTINCT(", column.name, ") as d FROM nis"))
  
  replace <- q %>% 
    filter(!is.na(d)) %>%
    filter(is.na(as.double(d))) %>%
    pull(d)
  
  if (length(replace) == 0) {
    print(paste0("Nothing to update for ", column.name))
    next
  }
  
  # Enclose in single quotes for query
  replace <- paste0("'", replace, "'")
  replace <- paste(replace, collapse=",")
  update.query <- paste0("UPDATE nis SET ", column.name, " = null WHERE ", column.name, " in (", replace, ")")
  print(update.query)
  
  DBI::dbSendQuery(con, update.query)
} 

# Create a temporary column 
column.names <- readLines('src/main/resources/metadata/convert-varchar-columns-to-integers.txt')
for (column in column.names) {
  column.tmp <- paste0(column, "_tmp")
  alter.query <- paste0("ALTER TABLE nis ADD COLUMN ", column.tmp, " INTEGER")
  print(alter.query)
  DBI::dbSendQuery(con, alter.query)
  
  # Set data in temporary column to original data 
  update.query <- paste0("UPDATE nis SET ", column.tmp, "=", column)
  print(update.query)
  DBI::dbSendQuery(con, update.query)
  
  # Remove the original column 
  alter.query.2 <- paste0("ALTER TABLE nis DROP COLUMN ", column)
  print(alter.query.2)
  DBI::dbSendQuery(con, alter.query.2)
  
  # Re-create the original column with the new type 
  alter.query.3 <- paste0("ALTER TABLE nis ADD COLUMN ", column, " INTEGER")
  print(alter.query.3)
  DBI::dbSendQuery(con, alter.query.3)
  
  # Move data from temporary column to new column 
  update.query.2 <- paste0("UPDATE nis set ", column, "=", column.tmp)
  print(update.query.2)
  DBI::dbSendQuery(con, update.query.2)
 
  # Drop the temporary column 
  alter.query.4 <- paste0("ALTER TABLE nis DROP COLUMN ", column.tmp)
  print(alter.query.4)
  DBI::dbSendQuery(con, alter.query.4)
  
  DBI::dbGetQuery(con, paste0("SELECT AVG(", column, ") FROM nis"))
} 

column.names <- readLines('src/main/resources/metadata/convert-varchar-columns-to-doubles.txt')
for (column in column.names) {
  column.tmp <- paste0(column, "_tmp")
  alter.query <- paste0("ALTER TABLE nis ADD COLUMN ", column.tmp, " DOUBLE PRECISION")
  print(alter.query)
  DBI::dbSendQuery(con, alter.query)
  
  # Set data in temporary column to original data 
  update.query <- paste0("UPDATE nis SET ", column.tmp, "=", column)
  print(update.query)
  DBI::dbSendQuery(con, update.query)
  
  # Remove the original column 
  alter.query.2 <- paste0("ALTER TABLE nis DROP COLUMN ", column)
  print(alter.query.2)
  DBI::dbSendQuery(con, alter.query.2)
  
  # Re-create the original column with the new type 
  alter.query.3 <- paste0("ALTER TABLE nis ADD COLUMN ", column, " DOUBLE PRECISION")
  print(alter.query.3)
  DBI::dbSendQuery(con, alter.query.3)
  
  # Move data from temporary column to new column 
  update.query.2 <- paste0("UPDATE nis set ", column, "=", column.tmp)
  print(update.query.2)
  DBI::dbSendQuery(con, update.query.2)
  
  # Drop the temporary column 
  alter.query.4 <- paste0("ALTER TABLE nis DROP COLUMN ", column.tmp)
  print(alter.query.4)
  DBI::dbSendQuery(con, alter.query.4)
  
  DBI::dbGetQuery(con, paste0("SELECT AVG(", column, ") FROM nis"))
} 


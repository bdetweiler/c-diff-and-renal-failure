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

library("devtools")
# NOTE:
# If you get this error on DBI::dbConnect()
#     Error: 'monetdb_embedded_startup' is not an exported object from 'namespace:MonetDBLite'
# run the install command below and try again. 
# install.packages( c("MonetDB.R", "MonetDBLite") , repos=c("http://dev.monetdb.org/Assets/R/", "http://cran.rstudio.com/"))
library('MonetDBLite')
library('dplyr')
library('DBI')

# setwd('/home/bdetweiler/src/Data_Science/stat-8960-capstone-project')

# Here, we modify the columns that were initialized as VARCHARs to accommodate for missing data codes
# into their ideal data types (typically INTEGER or DOUBLE) so we can offload calculations to the database
# We'll use the technique specified here: 
# https://stackoverflow.com/questions/27770370/alter-the-data-type-of-a-column-in-monetdb

# MonetDB connection to a permanent file
# Call the below line if you get an error about connecting
# MonetDBLite::monetdblite_shutdown()
con <- DBI::dbConnect(MonetDBLite::MonetDBLite(), "data/nrd_db")
# For all integer columns, replace non-integers with null
column.names <- readLines('src/main/resources/metadata/convert-varchar-columns-to-integers.txt')

existing.column.names <- DBI::dbGetQuery(con, paste0("SELECT * FROM nrd LIMIT 1"))
existing.column.names <- names(existing.column.names)
for (column.name in column.names) {
  
  if (!(column.name %in% existing.column.names)) {
    print(paste0("Skipping ", column.name, " - does not exist"))
    next
  }
  
  q <- DBI::dbGetQuery(con, paste0("SELECT DISTINCT(", column.name, ") as d FROM nrd"))

  replace <- q$d[which(is.na(as.integer(q$d)))]

  # Enclose in single quotes for query
  replace <- paste0("'", replace, "'")
  replace <- paste(replace, collapse=",")
  update.query <- paste0("UPDATE nrd SET ", column.name, " = null WHERE ", column.name, " in (", replace, ")")
  print(update.query)
  
  DBI::dbSendQuery(con, update.query)
} 

# For all double columns, replace non-doubles with null
column.names <- readLines('src/main/resources/metadata/convert-varchar-columns-to-doubles.txt')
for (column.name in column.names) {
  if (!(column.name %in% existing.column.names)) {
    print(paste0("Skipping ", column.name, " - does not exist"))
    next
  }
  
  q <- DBI::dbGetQuery(con, paste0("SELECT DISTINCT(", column.name, ") as d FROM nrd"))
  replace <- q$d[which(is.na(as.double(q$d)))]
  # Enclose in single quotes for query
  replace <- paste0("'", replace, "'")
  replace <- paste(replace, collapse=",")
  update.query <- paste0("UPDATE nrd SET ", column.name, " = null WHERE ", column.name, " in (", replace, ")")
  print(update.query)
  
  DBI::dbSendQuery(con, update.query)
} 

# Create a temporary column 

column.names <- readLines('src/main/resources/metadata/convert-varchar-columns-to-integers.txt')
for (column.name in column.names) {
  if (!(column.name %in% existing.column.names)) {
    print(paste0("Skipping ", column.name, " - does not exist"))
    next
  }
  
  column.tmp <- paste0(column.name, "_tmp")
  alter.query <- paste0("ALTER TABLE nrd ADD COLUMN ", column.tmp, " INTEGER")
  print(alter.query)
  DBI::dbSendQuery(con, alter.query)
  
  # Set data in temporary column to original data 
  update.query <- paste0("UPDATE nrd SET ", column.tmp, "=", column.name)
  print(update.query)
  DBI::dbSendQuery(con, update.query)
  
  # Remove the original column 
  alter.query.2 <- paste0("ALTER TABLE nrd DROP COLUMN ", column.name)
  print(alter.query.2)
  DBI::dbSendQuery(con, alter.query.2)
  
  # Re-create the original column with the new type 
  alter.query.3 <- paste0("ALTER TABLE nrd ADD COLUMN ", column.name, " INTEGER")
  print(alter.query.3)
  DBI::dbSendQuery(con, alter.query.3)
  
  # Move data from temporary column to new column 
  update.query.2 <- paste0("UPDATE nrd set ", column.name, "=", column.tmp)
  print(update.query.2)
  DBI::dbSendQuery(con, update.query.2)
 
  # Drop the temporary column 
  alter.query.4 <- paste0("ALTER TABLE nrd DROP COLUMN ", column.tmp)
  print(alter.query.4)
  DBI::dbSendQuery(con, alter.query.4)
  
  DBI::dbGetQuery(con, paste0("SELECT AVG(", column.name, ") FROM nrd"))
} 

column.names <- readLines('src/main/resources/metadata/convert-varchar-columns-to-doubles.txt')
for (column.name in column.names) {
  
  if (!(column.name %in% existing.column.names)) {
    print(paste0("Skipping ", column.name, " - does not exist"))
    next
  }
  
  column.tmp <- paste0(column, "_tmp")
  alter.query <- paste0("ALTER TABLE nrd ADD COLUMN ", column.tmp, " DOUBLE PRECISION")
  print(alter.query)
  DBI::dbSendQuery(con, alter.query)
  
  # Set data in temporary column to original data 
  update.query <- paste0("UPDATE nrd SET ", column.tmp, "=", column.name)
  print(update.query)
  DBI::dbSendQuery(con, update.query)
  
  # Remove the original column 
  alter.query.2 <- paste0("ALTER TABLE nrd DROP COLUMN ", column.name)
  print(alter.query.2)
  DBI::dbSendQuery(con, alter.query.2)
  
  # Re-create the original column with the new type 
  alter.query.3 <- paste0("ALTER TABLE nrd ADD COLUMN ", column.name, " DOUBLE PRECISION")
  print(alter.query.3)
  DBI::dbSendQuery(con, alter.query.3)
  
  # Move data from temporary column to new column 
  update.query.2 <- paste0("UPDATE nrd set ", column.name, "=", column.tmp)
  print(update.query.2)
  DBI::dbSendQuery(con, update.query.2)
  
  # Drop the temporary column 
  alter.query.4 <- paste0("ALTER TABLE nrd DROP COLUMN ", column.tmp)
  print(alter.query.4)
  DBI::dbSendQuery(con, alter.query.4)
  
  DBI::dbGetQuery(con, paste0("SELECT AVG(", column.name, ") FROM nrd"))
}

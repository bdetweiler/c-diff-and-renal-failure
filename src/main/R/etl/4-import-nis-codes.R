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
# Imports NIS codes and descriptions as well as the format type:
#    i9dxf  - ICD-9 Diagnosis codes
#    i9prf  - ICD-9 Procedure codes
#    i10dxf - ICD-10 Diagnosis codes
#    i10prf - ICD-10 Procedure codes
# Note: codes of interest for this project are
#    00845;i9dxf;CLOSTRIDIUM DIF (Begin 1992)
#    A047;i10dxf;Enterocolitis due to Clostridium difficile
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

#setwd('/home/bdetweiler/src/Data_Science/stat-8960-capstone-project/')
# con <- DBI::dbConnect(MonetDBLite::MonetDBLite(), "data/nis_db")
create.nis.codes.table <- readLines('src/main/resources/sql/nis-codes-create-table.sql')
create.nis.codes.table <- paste0(create.nis.codes.table, collapse=' ')

# Create nis_code table
DBI::dbSendQuery(con, create.nis.codes.table)

# I10 Procedure codes
i10prf.codes.df <- read_delim('data/nis-codes-i10prf.csv', delim=';')

DBI::dbWriteTable(con, "nis_code", i10prf.codes.df, append=TRUE, row.names=FALSE)

i10dxf.codes.df <- read.csv('data/nis-codes-i10dxf.csv', sep=';')

# I10 Procedure codes
i10dxf.codes.df <- read_delim('data/nis-codes-i10dxf.csv', delim=';')

DBI::dbWriteTable(con, "nis_code", i10dxf.codes.df, append=TRUE, row.names=FALSE)

# I9 Procedure codes
i9prf.codes.df <- read_delim('data/nis-codes-i9prf.csv', delim=';', col_types="ccc")

DBI::dbWriteTable(con, "nis_code", i9prf.codes.df, append=TRUE, row.names=FALSE)

# I9 Diagnosis codes
i9dxf.codes.df <- read_delim('data/nis-codes-i9dxf.csv', delim=';')

DBI::dbWriteTable(con, "nis_code", i9dxf.codes.df, append=TRUE, row.names=FALSE)

MonetDBLite::monetdblite_shutdown()
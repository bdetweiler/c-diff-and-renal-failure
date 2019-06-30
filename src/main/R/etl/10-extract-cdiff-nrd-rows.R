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
# Queries all c. diff entries and then pulls out all entries with that NRD Link.
# The result is a CSV with all visits from patients who have been diagnosed with c. diff.
################################################################################################

# install.packages( c("MonetDB.R", "MonetDBLite") , repos=c("http://dev.monetdb.org/Assets/R/", "http://cran.rstudio.com/"))
library('MonetDBLite')
library('tidyverse')
library('DBI')
library('beepr')

setwd('/home/bdetweiler/src/Data_Science/c-diff-and-renal-failure/')

source('src/main/R/util/verify-working-directory.R')

tryCatch({
  path <- getwd()
  verify_wd(path, "c-diff-and-renal-failure")
}, warning = function(e) {
  stop("Ensure working directory is set to `c-diff-and-renal-failure`.")
}, error = function(e) {
  stop("Ensure working directory is set to `c-diff-and-renal-failure`.")
})

con <- DBI::dbConnect(MonetDBLite::MonetDBLite(), "data/nrd_db")

get.cdiff.nrd.sql <- paste0(readLines('src/main/resources/sql/nrd-get-all-cdiff.sql'), collapse = " ")
print(get.cdiff.nrd.sql)

q <- DBI::dbGetQuery(con, get.cdiff.nrd.sql)

write_csv(q, path = 'data/cdiff-nrd.csv')

beep(3)
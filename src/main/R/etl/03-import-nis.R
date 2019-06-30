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
# Imports NIS data from raw CSV files. 
# NOTE: If adding a new year (2015+), you will need
# to open the headers.xlsx file and manually add the header metadata. Once you have
# done this, create two files - one called headers-nis20xx.txt, with the year you are
# adding. And one called r-data-types-nis20xx.txt containing the R data type corresponding
# to each column. If a new column is being added, you will have to go back to step 1 and
# create the column in the first step. Then you will have to adjust all the headers and
# data type files to reflect the column addition.
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

# Get all headers from all years 2001-2014 in the order they appear in the database
all.headers <- readLines('src/main/resources/metadata/all-headers-nis.txt')

# We have generated the headers and renamed a few manually - see src/main/resources/metadata/headers.xlsx
years <- seq(from = 2001, to = 2014, by = 1)

for (year in years) {

  print(paste("Processing year", year))
  data.types.file <- paste0('src/main/resources/metadata/r-data-types-nis', year, '.txt')
  data.types <- readLines(data.types.file)

  data.types <- gsub("character", "c", data.types, "ALL")
  data.types <- gsub("integer", "i", data.types, "ALL")
  data.types <- gsub("double", "d", data.types, "ALL")
  data.types <- tibble(data.types) %>% 
    filter(data.types != "") %>% 
    pull(data.types)
  data.types
  data.types <- paste0(data.types, collapse="")

  # Copy and paste the headers from the 2001 tab in headers.xlsx into a text file
  headers.file <- paste0('src/main/resources/metadata/headers-nis', year, '.txt')
  headers <- readLines(headers.file)
  headers <- tibble(headers) %>% filter(headers != '') %>% pull(headers)

  # XXX: This takes about 5 minutes to read on my Intel i7
  # Note: We specify the header names because some will be different in the database - namely KEY (NIS_KEY) and YEAR (NIS_YEAR)
  # Note: We specify the colClasses beause these are specific to the database
  nis.file <- paste0('data/NIS', year, '.csv')

  nis <- read_csv(nis.file, 
                  col_names = headers,
                  col_types = data.types,
                  skip = 1)

  # NIS_KEY, NIS_YEAR, and AGE are expected to exist in all years
  # Compare what's in the CSV to all headers. 
  missing.headers <- c()
  for (column in all.headers) {
    if (!(column %in% colnames(nis))) {
      missing.headers <- c(missing.headers, column)
      nis <- cbind(nis, as.character(c(NA)))
    }
  }

  all.headers.out.of.order <- c(headers, missing.headers)

  names(nis) <- all.headers.out.of.order
  
  # Sort columns in correct order 
  nis <- nis %>% select(all.headers)

  # MonetDB connection to a permanent file
  # Call the below line if you get an error about connecting
  # MonetDBLite::monetdblite_shutdown()
  
  # Write the CSV file to the database
  DBI::dbWriteTable(con, "nis", nis, append = TRUE, row.names = FALSE)
} 

beep(sound=3)

MonetDBLite::monetdblite_shutdown()

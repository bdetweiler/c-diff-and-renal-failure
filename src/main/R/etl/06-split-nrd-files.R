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
# Even with 64GB of RAM, the NRD files are too large when merging, so we have to split them.
# We'll also get rid of the V2 in 2010-2013 to keep things consistent.
#
# 2015 has two separate files for Q1-Q3 and Q4.
#
# If adding data for 2016 and beyond, you will need to edit each part of this script
# as I am not sure what the format will look like.
################################################################################################

library('tidyverse')
library('beepr')

source('src/main/R/util/verify-working-directory.R')

tryCatch({
  path <- getwd()
  verify_wd(path, "c-diff-and-renal-failure")
}, warning = function(e) {
  stop("Ensure working directory is set to `c-diff-and-renal-failure`.")
}, error = function(e) {
  stop("Ensure working directory is set to `c-diff-and-renal-failure`.")
})

# Ratio of how to split the large files. Default is 0.25, which will split each file into 4 smaller files.
# For machines with less RAM, you can make this smaller
FILE.SPLIT.RATIO <- 0.25

nrdyears <- system("ls data | egrep 'NRD[1-2][0-9][0-9][0-9]'", intern=TRUE)

for (nrdyear in nrdyears) {
  
  year <- gsub("NRD", "", nrdyear)

  #####################################################################################
  #                               CORE  
  #####################################################################################
  core.file <- system(paste0("ls data/", nrdyear, " | egrep '.*Core.*CSV'"), intern=TRUE)

  line.count <- system(paste0("wc -l data/", nrdyear, "/", core.file), intern=TRUE)
  line.count <- gsub(' .*', "", line.count)
  line.count <- as.integer(line.count)
 
  reduced.line.count <- ceiling(line.count * FILE.SPLIT.RATIO)

  system(paste0("split -l", reduced.line.count, " -d data/", nrdyear, "/", core.file, " data/", nrdyear, "/NRD_", year, "_Core_split_"), intern=FALSE)
  
  #####################################################################################
  #                               DX_PR_GRPS  
  #####################################################################################
  dx.pr.files <- system(paste0("ls data/", nrdyear, " | egrep '.*DX_PR_GRPS.*CSV'"), intern=TRUE)

  # 2015 has files for Q1-Q3 and Q4 separately
  for (dx.pr.file in dx.pr.files)  {
    line.count <- system(paste0("wc -l data/", nrdyear, "/", dx.pr.file), intern=TRUE)
    line.count <- gsub(' .*', "", line.count)
    line.count <- as.integer(line.count)

    quarters <- "" 
    if (grepl("Q1Q3", dx.pr.file, fixed = TRUE)) {
      quarters <- "Q1Q3" 
    } else if (grepl("Q4", dx.pr.file, fixed = TRUE)) {
      quarters <- "Q4" 
    }

    reduced.line.count <- ceiling(line.count * FILE.SPLIT.RATIO)

    system(paste0("split -l", reduced.line.count, " -d data/", nrdyear, "/", dx.pr.file, " data/", nrdyear, "/NRD_", year, quarters, "_DX_PR_GRPS_split_"), intern=FALSE)
  }
  
  #####################################################################################
  #                               SEVERITY  
  #####################################################################################
  severity.files <- system(paste0("ls data/", nrdyear, " | egrep '.*Severity.*CSV'"), intern=TRUE)

  # 2015 has files for Q1-Q3 and Q4 separately
  for (severity.file in severity.files)  {
    line.count <- system(paste0("wc -l data/", nrdyear, "/", severity.file), intern=TRUE)
    line.count <- gsub(' .*', "", line.count)
    line.count <- as.integer(line.count)

    quarters <- "" 
    if (grepl("Q1Q3", severity.file, fixed = TRUE)) {
      quarters <- "Q1Q3" 
    } else if (grepl("Q4", severity.file, fixed = TRUE)) {
      quarters <- "Q4" 
    }

    reduced.line.count <- ceiling(line.count * FILE.SPLIT.RATIO)

    system(paste0("split -l", reduced.line.count, " -d data/", nrdyear, "/", severity.file, " data/", nrdyear, "/NRD_", year, quarters, "_Severity_split_"), intern=FALSE)
  }
  
}

# Play fanfair to let us know when this is done so we can move on to the next step
beep(3)

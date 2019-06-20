#!/bin/bash
date

pid=`ps aux | grep "R --slave" | grep -v grep | awk '{print $2}'`
echo "PID: ${pid}"
cursor=`cat /home/bdetweiler/src/Data_Science/c-diff-and-renal-failure/src/main/resources/last-position.txt  | sed -e 's/.*= //'`
echo "CURSOR: ${cursor}"

export RHOME="/usr/lib/R"
export R_HOME="/usr/lib/R"

if [[ $pid = *[!\ ]* ]]; then
    if [[ $pid -gt 0 ]]; then
        echo "Everything is good! PID: ${pid}"
    else
        date >> crashreport.log
        echo "Script is down! Starting again..." 
        /usr/lib/R/bin/exec/R --slave --no-restore --file=/home/bdetweiler/src/Data_Science/c-diff-and-renal-failure/src/main/R/etl/9-import-icd-codes.R 
    fi
else
    echo "Checking if ${cursor} < 274737 == $((${cursor} < 274737))"
    if (( ${cursor} < 274737 )); then
        echo "Apparently ${cursor} < 274737 == $((${cursor} < 274737))"
        echo "Starting script again..."
        date >> crashreport.log
        /usr/lib/R/bin/exec/R --slave --no-restore --file=/home/bdetweiler/src/Data_Science/c-diff-and-renal-failure/src/main/R/etl/9-import-icd-codes.R 
    else
        echo "Apparently ${cursor} < 274737 == $((${cursor} < 274737))"
        echo "Script is done!!!"
    fi
fi


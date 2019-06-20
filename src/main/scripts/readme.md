# Install the crontab to keep the script running in case it crashes

## Fillin the path to your project

*/10 * * * * /bin/bash <<<PATH TO YOUR PROJECT>>>/c-diff-and-renal-failure/src/main/scripts/checkR.sh >> <<<PATH TO YOUR PROJECT>>>/c-diff-and-renal-failure/src/main/scripts/cron.log

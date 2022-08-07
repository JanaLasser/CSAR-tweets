#!/bin/bash

cd /home/jlasser/CSAR-tweets/code
source server_settings.txt

OFFSET=$1
DAY=$(date +%Y-%m-%d -d  "${OFFSET} hour ago")
HOUR=$(date +%H -d  "${OFFSET} hour ago")
echo $TMP_STORAGE/$DAY/$HOUR

for file in $TMP_STORAGE/$DAY/$HOUR/*.jsonl
do
    echo "[]" | ./create_csv_head.jq > "${file%.jsonl}.csv"
    cat "$file" | ./parse_tweets.jq >> "${file%.jsonl}.csv"
    rm "$file"
done

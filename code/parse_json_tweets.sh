#!/bin/bash

cd /home/jlasser/CSAR-tweets/code
source server_settings.txt

DAY=$(date +%Y-%m-%d -d  "1 hour ago")
HOUR=$(date +%H -d  "1 hour ago")

for file in $TMP_STORAGE/$DAY/$HOUR/*.jsonl
do
    echo "[]" | ./create_csv_head.jq > "${file%.jsonl}.csv"
    cat "$file" | ./parse_tweets.jq >> "${file%.jsonl}.csv"
    rm "$file"
done



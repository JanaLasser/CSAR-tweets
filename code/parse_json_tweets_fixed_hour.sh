#!/bin/bash

source server_settings.txt
cd $REPOSITORY_DST/code

DAY=$(date +%Y-%m-%d -d  "1 hour ago")
HOUR=$(date +%H -d  "1 hour ago")
echo $TMP_STORAGE/$DAY/$HOUR

for file in $TMP_STORAGE/$DAY/$HOUR/*.jsonl
do
    echo "[]" | ./create_csv_head.jq > "${file%.jsonl}.csv"
    cat "$file" | ./parse_tweets.jq >> "${file%.jsonl}.csv"
    rm "$file"
done

#!/bin/bash

cd /home/jlasser/CSAR-tweets/code
source server_settings.txt

./parse_json_tweets.sh
$PYTHON_DST/python collect_data.py $REPOSITORY_DST/code/


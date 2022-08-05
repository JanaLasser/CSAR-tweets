#!/bin/bash

source server_settings.txt
cd $REPOSITORY_DST/code

./parse_json_tweets.sh
$PYTHON_DST/python collect_data.py $REPOSITORY_DST/code/


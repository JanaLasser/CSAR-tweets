import os
from os.path import join
import sys
import json
import datetime
import socket
import pwd
import grp

## load the settings for the server we are running on ##
# cwd is passed via the command line, since when running as a service we can't
# get the current working directory via os.getcwd()
cwd = sys.argv[1] 
server_settings = {}
with open(join(cwd, "server_settings.txt"), 'r') as f:
    for l in f:
        server_settings[l.split('=')[0]] = l.split('=')[1].strip('\n')

# insert the library destination into the pythonpath and load third-party libs
sys.path.insert(0, server_settings["PYTHON_LIBRARY_DST"])
import pandas as pd
from twarc import Twarc2
from twarc.expansions import flatten

# custom functions for the stream scraper
import helper_functions as hf

# setting up notifications, Twitter API access credentials, 
# file ownership information and file I/O paths
notifications = server_settings["NOTIFICATIONS"]
notifications = {"True":True, "False":False}[notifications]
if notifications:
    email_credentials_dst = server_settings["EMAIL_CREDENTIALS_DST"]
    email_credentials_filename = server_settings["EMAIL_CREDENTIALS_FILENAME"]

project_name = server_settings["PROJECT_NAME"]    
API_key_dst = server_settings["TWITTER_API_KEY_DST"]
API_key_filename = server_settings["TWITTER_API_KEY_FILENAME"]
data_storage_dst = server_settings[f"TMP_STORAGE"]
username = server_settings["USERNAME"]
groupname = server_settings["GROUPNAME"]
uid = pwd.getpwnam(username).pw_uid
gid = grp.getgrnam(groupname).gr_gid
host = socket.gethostname()
credentials = hf.get_twitter_API_credentials(
    filename=API_key_filename, 
    keydst=API_key_dst)
bearer_token = credentials["bearer_token"]
client = Twarc2(bearer_token=bearer_token)

# check if all rules are still on
hf.check_rules(client, cwd)

dumptime = 60 # time [in seconds] at which the stream is dumped to disk

tweets = []
start = datetime.datetime.now()
header = f"[NOTICE] started {project_name} tweet stream on {host}!"
if notifications:
    hf.notify(
        header,
        str(start), 
        credential_src=email_credentials_dst,
        credential_fname=email_credentials_filename
    )
else:
    print(header)

try:
    while True:
        for tweet in client.stream(
                event=None, 
                record_keepalive=True, 
                expansions=",".join(hf.EXPANSIONS), 
                tweet_fields=",".join(hf.TWEET_FIELDS),
                user_fields=",".join(hf.USER_FIELDS),
                media_fields=[],
                poll_fields=[],
                place_fields=[],
                backfill_minutes=5
        ):
            if tweet == 'keep-alive':
                print("staying alive ...")
            else:
                try:
                    tweet = flatten(tweet)[0]
                    if not "referenced_tweets" in tweet.keys():
                        tweet["referenced_tweets"] = [{'type': 'original',
                                                       'id': None}]
                    tweets.append(tweet)
                except Exception as e:
                    print(e)

            now = datetime.datetime.now()
            diff = (now - start).seconds
            if diff > dumptime: # dump tweets every minute
                print("dumping tweets")
                hf.dump_tweets(tweets, start, now, data_storage_dst, uid, gid)
                tweets = []
                start = datetime.datetime.now()
                
except Exception as e:
    header = f"[WARNING] {project_name} stream terminated on {host}!"
    if notifications:
        ssf.notify(
            header,
            str(e),
            credential_src=email_credentials_dst,
            credential_fname=email_credentials_filename
        )
    else:
        print(header)
        
        

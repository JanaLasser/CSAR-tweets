import os
from os.path import join
import datetime
import sys
import osfclient
import pandas as pd

## load the settings for the server we are running on ##
# cwd is passed via the command line, since when running as a service we can't
# get the current working directory via os.getcwd()
cwd = sys.argv[1] 
server_settings = {}
with open(join(cwd, "server_settings.txt"), 'r') as f:
    for l in f:
        server_settings[l.split('=')[0]] = l.split('=')[1].strip('\n')

storage = "osfstorage" # seems to be the name of the default OSF storage provider
project_ID = server_settings["OSF_PROJECT_ID"] # get this from the URL of the project in the browser        
        
# file I/O paths and folder names
src = server_settings["DATA_VAULT_DST"]
prev_day = datetime.datetime.today() - datetime.timedelta(hours=24)

yearmonthday = "{}-{:02d}-{:02d}"\
    .format(prev_day.year, prev_day.month, prev_day.day)
year = "{:04d}".format(prev_day.year)
month = "{:02d}".format(prev_day.month)
day = "{:02d}".format(prev_day.day)

local_path = join(src, yearmonthday)

# collect all files from the last day and combine them into a single file
hour_files = [f"{yearmonthday}_{hour:02d}_tweets.csv.xz" for hour in range(0, 24)]
hour_files.sort()
day_tweets = pd.DataFrame()
for f in hour_files:
    try:
        df = pd.read_csv(
            join(src, yearmonthday, f),
            compression="xz",
            dtype={"id":str, "author_id":str, "conversation_id":str}
        )
        day_tweets = pd.concat([day_tweets, df])
    except FileNotFoundError:
        print(f"file {f} not found")
        pass
    
day_tweets.to_csv(
    join(src, yearmonthday, f"{yearmonthday}_tweets.csv.xz"),
    compression="xz",
    index=False
)
# clean up
#for f in hour_files:
#    os.remove(join(src, yearmonthday, f))
    
## set up the OSF client ##
# load the credentials
osf_credentials = {}
with open(
    join(server_settings["OSF_KEY_DST"],
         server_settings["OSF_KEY_FILENAME"]), "r") as credfile:
    for l in credfile:
        osf_credentials[l.split("=")[0]] = l.split("=")[1].strip("\n")     

# initialize the client
osf = osfclient.OSF(
    username=osf_credentials["username"],
    token=osf_credentials["token"]
)
# initialize the project (can also be a "component" of a larger project)
project = osf.project(project_ID)
store = project.storage(storage)

## upload tweets and the daily report to the repository
fname_tweets = f"{yearmonthday}_tweets.csv.xz"
fname_report = f"{yearmonthday}_report.txt"
for fname in [fname_tweets, fname_report]:
    if os.path.exists(join(local_path, fname)):
        with open(join(local_path, fname), 'rb') as fp:
            try:
                store.create_file(fname, fp, force="f")
            except FileExistsError:
                print(f"not writing {fname} because file exists at OSF")
    else:
        print(f"not uploading {join(local_path, fname)} because file doesn't exist locally")

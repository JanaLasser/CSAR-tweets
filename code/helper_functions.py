from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import smtplib
import os
import sys
from os.path import join
import json
import pandas as pd


USER_FIELDS = [
    "created_at",
    #"description",
    #"entities",
    "id",
    "location",
    "name",
    #"pinned_tweet_id",
    #"profile_image_url",
    "protected",
    "public_metrics",
    #"url",
    "username",
    "verified",
    #"withheld",
]

TWEET_FIELDS = [
    #"attachments",
    "author_id",
    #"context_annotations",
    "conversation_id",
    "created_at",
    #"entities",
    #"geo",
    "id",
    #"in_reply_to_user_id",
    "lang",
    #"public_metrics",
    "text",
    #"possibly_sensitive",
    "referenced_tweets",
    #"reply_settings",
    "source",
    #"withheld",
]

EXPANSIONS = ["author_id"]

DTYPES = {
    "id": str, 
    "conversation_id":str,
    "author_id":str,
    #"created_at":str,
    #"retrieved_at":str, 
    "source":str,
    "lang":str,
    "text":str,
    "reference_type":str,
    "referenced_tweet_id":str,
    #"author.created_at":str,
    "author.location":str, 
    "author.name":str, 
    "author.username":str, 
    "author.verified":str, 
    "author.protected":str,
    "author.public_metrics.followers_count":float, 
    "author.public_metrics.following_count":float,
    "author.public_metrics.tweet_count":float,
    "author.public_metrics.listed_count":float}

AUTHOR_COLS = [
    "author_id", "lang", "author.created_at", "author.location",
    "author.name", "author.username", "author.verified",
    "author.protected", "author.public_metrics.followers_count",
    "author.public_metrics.following_count",
    "author.public_metrics.tweet_count",
    "author.public_metrics.listed_count"]


def get_twitter_API_credentials(filename="twitter_API_jana.txt", keydst="twitter_API_keys"):
    '''
    Returns the bearer tokens to access the Twitter v2 API for a list of users.
    '''
    credentials = {}
    with open(join(keydst, filename), 'r') as f:
        for l in f:
            if l.startswith("bearer_token"):
                credentials[l.split('=')[0]] = l.split('=')[1].strip('\n')
    return credentials


def notify(subject, body, credential_src=os.getcwd(), 
           credential_fname="email_credentials.txt"):
    '''
    Writes an email with the given subject and body from a mailserver specified
    in the email_credentials.txt file at the specified location. The email
    address to send the email to is also specified in the credentials file.
    '''
    email_credentials = {}
    with open(join(credential_src, credential_fname), "r") as f:
        for line in f.readlines():
            line = line.strip("\n")
            email_credentials[line.split("=")[0]] = line.split("=")[1]
            
    fromaddr = email_credentials["fromaddr"]
    toaddr = email_credentials["toaddr"]
    msg = MIMEMultipart()
    msg['From'] = fromaddr
    msg['To'] = toaddr
    msg['Subject'] = subject
    msg.attach(MIMEText(body, 'plain'))

    server = smtplib.SMTP(
        email_credentials["server"],
        int(email_credentials["port"])
    )
    server.ehlo()
    server.starttls()
    server.ehlo()
    server.login(email_credentials["user"], email_credentials["password"])
    text = msg.as_string()
    server.sendmail(fromaddr, toaddr, text)


def dump_tweets(tweets, t1, t2, dst, uid, gid):
    '''Save a list of tweets as binary line-separated json'''
    
    daydirname = "{}-{:02d}-{:02d}".format(t1.year, t1.month, t1.day)
    hourdirname = "{:02d}".format(t1.hour)

    if not os.path.exists(join(dst, daydirname)):
        os.mkdir(join(dst, daydirname))
        os.chown(join(dst, daydirname), uid, gid)
    
    
    if not os.path.exists(join(dst, daydirname, hourdirname)):
        os.mkdir(join(dst, daydirname, hourdirname))
        os.chown(join(dst, daydirname, hourdirname), uid, gid)
    
    datetime1 = "{}-{:02d}-{:02d}_{:02d}:{:02d}:{:02d}"\
        .format(t1.year, t1.month, t1.day, t1.hour, t1.minute, t1.second)
    datetime2 = "{}-{:02d}-{:02d}_{:02d}:{:02d}:{:02d}"\
        .format(t2.year, t2.month, t2.day, t2.hour, t2.minute, t2.second)
        
    fname = f"CSAR_stream_{datetime1}_to_{datetime2}.jsonl"

    with open(join(dst, daydirname, hourdirname, fname), 'wb') as f:
        for tweet in tweets:
            json_str = json.dumps(tweet) + "\n"
            json_bytes = json_str.encode('utf-8')
            f.write(json_bytes)
            
    os.chown(join(dst, daydirname, hourdirname, fname), uid, gid)
    
    
def get_hour_files(hour_dst):
    all_hour_files = os.listdir(hour_dst)
    hour_files = [f for f in all_hour_files if f.endswith(".csv")]
    
    if len(all_hour_files) != len(hour_files):
        print(f"too many files in {hour_dst}")
        
    hour_tweets = pd.DataFrame()
    for f in hour_files:
        tmp = pd.read_csv(
            join(hour_dst, f), 
            #error_bad_lines=False, 
            dtype=DTYPES, 
            parse_dates=["created_at", "retrieved_at", "author.created_at"]
        )
        hour_tweets = pd.concat([hour_tweets, tmp])
    hour_tweets = hour_tweets.reset_index(drop=True)
    return hour_tweets

def build_rules_parliamentarians(handles):
    '''
    Builds rules for the twitter API sampled stream given a number
    of user handles from whom tweets should be streamed. Respects
    the maximum number of 1024 characters per rule and starts a
    new rule if the limit is reached.
    '''
    rules = []
    curr_rule = "from:{}".format(handles[0])
    handle_index = 0
    for handle in handles[1:]:
        if len(curr_rule) + len(handle) + 9 < 1024:
            curr_rule += f" OR from:{handle}"
        else:
            rules.append(curr_rule)
            curr_rule = f"from:{handle}"
            
    return rules


def build_rules_selectors(src):
    '''
    Builds rules for the twitter API filtered stream given user handles,
    words, phrases and hashtags for the "pro-CSAR", "contra-CSAR" and 
    "neutral-CSAR" factions.
    '''
    faction_rules = {"pro":"", "con":"", "neu":""}

    for faction in faction_rules.keys():
        # users:
        curr_rule = ""
        handles = []
        with open(join(src, f"selectors_{faction}_users.txt"), "r") as infile:
            for line in infile.readlines():
                handles.append(line.strip("\n"))
        if len(handles) == 0:
            pass
        elif len(handles) == 1:
            curr_rule = "from:{}".format(handles[0])
        else:
            curr_rule = "from:{}".format(handles[0])
            for handle in handles[1:]:
                curr_rule += f" OR from:{handle}"

        # words
        words = []
        with open(join(src, f"selectors_{faction}_words.txt"), "r") as infile:
            for line in infile.readlines():
                words.append(line.strip("\n"))
        if len(curr_rule) > 0 and len(words) > 0:
            curr_rule += " OR "

        if len(words) == 0:
            pass
        elif len(words) == 1:
            curr_rule += words[0]
        else:
            curr_rule += "{}".format(words[0])
            for word in words[1:]:
                curr_rule += f" OR {word}"

        # phrases
        phrases = []
        with open(join(src, f"selectors_{faction}_phrases.txt"), "r") as infile:
            for line in infile.readlines():
                phrases.append(line.strip("\n"))
        if len(curr_rule) > 0 and len(phrases) > 0:
            curr_rule += " OR "

        if len(phrases) == 0:
            pass
        elif len(phrases) == 1:
            curr_rule += '"{}"'.format(phrases[0])
        else:
            curr_rule += '"{}"'.format(phrases[0])
            for phrase in phrases[1:]:
                curr_rule += f' OR "{phrase}"'

        # hashtags
        hashtags = []
        with open(join(src, f"selectors_{faction}_hashtags.txt"), "r") as infile:
            for line in infile.readlines():
                hashtags.append(line.strip("\n"))
        if len(curr_rule) > 0 and len(hashtags) > 0:
            curr_rule += " OR "

        if len(hashtags) == 0:
            pass
        elif len(hashtags) == 1:
            curr_rule += "{}".format(hashtags[0])
        else:
            curr_rule += "{}".format(hashtags[0])
            for hashtag in hashtags[1:]:
                curr_rule += f" OR {hashtag}"

        faction_rules[faction] += curr_rule

    return faction_rules


def delete_all_rules(client):
    IDs = client.get_stream_rules()["data"]
    IDs = [r["id"] for r in IDs]
    client.delete_stream_rule_ids(IDs)
    print(f"deleted {len(IDs)} rules from stream")
    
    
def check_rules(client, cwd):
    '''
    Checks if the currently active rules are equal to the rules
    specified in the rules files contained in the rules folder.
    If the rules are not equal, this function ensures rules are
    deleted or added until the active rules match the expected ones.
    '''
    # get all currently running rules
    try:
        curr_rule_tags = client.get_stream_rules()["data"]
        curr_rule_tags = set([r["tag"] for r in curr_rule_tags])
    except KeyError:
        curr_rule_tags = set()

    # get all rules that should be running from the directory of rule files
    wanted_rule_tags = os.listdir(join(cwd, "rules"))
    wanted_rule_tags = [r for r in wanted_rule_tags if "rule" in r]
    wanted_rule_tags = set(["CSAR tweets " + r.split(".")[0].replace("_", " ") \
                        for r in wanted_rule_tags])

    # current rules are equal to expected rules: no action
    if curr_rule_tags == wanted_rule_tags:
        print("No rules are missing.")
        pass
    # somehow there are more rules active then expected: delete all rules
    # and set only the expected rules as new rules
    elif len(curr_rule_tags) > len(wanted_rule_tags):
        print("Found too many active rules. Resetting all rules.")
        hf.delete_all_rules(client)
        rule_files = os.listdir(join(cwd, "rules"))
        rule_files = [f for f in rule_files if "rule" in f]
        rule_files.sort()
        rules = []
        for rule_filename in rule_files:
            with open(join(cwd, f"rules/{rule_filename}"), "r") as rule_file:
                rule = rule_file.read()
                rules.append({"value":rule, "tag":"CSAR tweets " + \
                              rule_filename.split(".")[0].replace("_", " ")})
        client.add_stream_rules(rules)

    # there are fewer rules active than expected: find the missing rules
    # and set them up
    elif len(curr_rule_tags) < len(wanted_rule_tags):
        missing_rule_tags = wanted_rule_tags.difference(curr_rule_tags)
        print(f"{len(missing_rule_tags)} rules are missing. Adding missing rules.")
        missing_rules = []
        for missing_rule_tag in missing_rule_tags:
            missing_rule_number = missing_rule_tag.split(" ")[-1]
            with open(join(cwd, "rules/rule_{}.txt".format(missing_rule_number)), "r") as rule_file:
                missing_rule = rule_file.read()
                missing_rules.append({"value":missing_rule, "tag":"CSAR tweets rule " + \
                                           missing_rule_number})

        client.add_stream_rules(missing_rules)
        
    # This should not happen. Ever.
    else:
        print("Something extremely unexpected has just happened. Exiting.")
        sys.exit()
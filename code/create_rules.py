import pandas as pd
import helper_functions as hf

# see https://freepad.erdgeist.org/p/chatcontrol-h76R_92hLpQ83%26s for a living 
# doc with the list of selectors

src = "../data"
faction_rules = hf.build_rules_selectors(src)

handles = pd.read_csv("../data/EU_parliament_twitter_accounts.csv", usecols=["username"])\
    ["username"].values
politician_rules = hf.build_rules_parliamentarians(handles)

assert len(politician_rules) + len(faction_rules) < 1000 # max 1000 rules / account

rules = list(faction_rules.values()) + politician_rules

for rule in rules:
    assert len(rule) <= 1024 # max 1024 characters / rule

        
for i, rule in enumerate(politician_rules):
    with open(f"rules/politician_rule_{i:02d}.txt", "w") as rulefile:
        rulefile.write(rule)
    
for faction, rule in faction_rules.items():
    with open(f"rules/selector_rule_{faction}.txt", "w") as rulefile:
        rulefile.write(rule)
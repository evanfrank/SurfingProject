import json
import os

import pandas

import requests

newstories_url = "https://hacker-news.firebaseio.com/v0/topstories.json"
top_new_story_ids = requests.get(newstories_url).json()[:100]

os.makedirs("data", exist_ok=True)
with open("data/topstory_ids.json", "w") as f:
    json.dump(top_new_story_ids, f)

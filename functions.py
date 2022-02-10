from textblob import TextBlob
import os  
# import subprocess

def input_lists(name_dict):
    name_list, id, twitter_handles= [], [], []
    for key in name_dict.keys():
        name_list.append(key)
    for i, name in enumerate(name_list):
        twitter_handles.append(name_dict[name]["Twitter"])
    for i, name in enumerate(name_list):
        id.append(name_dict[name]["ID"])
    return twitter_handles, name_list, id

"""Using the OS library to call CLI commands in Python"""
def scraper(search_from, search_term, filename): 
    # subprocess.run(f'snscrape --jsonl --progress --since {str(search_from)} twitter-search "{str(search_term)} lang:en" > {filename}')
    os.system(f'snscrape --jsonl --progress --since {str(search_from)} twitter-search "{str(search_term)} lang:en" > {filename}')


def polarity(text): 
    polar = TextBlob(text).sentiment.polarity
    return polar
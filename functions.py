from textblob import TextBlob
import scraper_input_data as dt
import pandas as pd
import datetime

# timeframe entered in minutes
timeframe = 60
# conversion to seconds for intereaction with second counter to limit api request
secs = timeframe * 60

class TweetAnalyzer():
    def __init__(self, df):
        self.df = df

    def tweets_to_data_frame(self, tweets, name):
        for tweet in tweets:
            #Account for daylight savings
            if ((datetime.datetime.now() - tweet.created_at.utcnow()).total_seconds()-3600) < secs:
                self.df = self.df.append({'Polarity - {}'.format(name): (TextBlob(tweet.text)).sentiment.polarity},ignore_index=True)
        return self.df 

def labels(name_dict):
    lebels = []
    for name in name_dict:
        for label in name_dict[name]: 
            if label not in lebels:
                lebels.append(label)
    return lebels

def name_list(name_dict): 
    name_list = []
    names = dt.name_dict
    for key in names.keys():
        name_list.append(key)
    return name_list

def twitter_handles(name_dict):
    twitter_handles = []
    names = name_list(name_dict)
    for i, name in enumerate(names):
            twitter_handles.append(name_dict[name]["Twitter"])
    return twitter_handles

def Influencers(name_dict):
    Influencers = []
    for i, name in enumerate(name_dict):
        Influencers.append(name_dict[name]["Influencers"])
    return Influencers

def twitter_handles_no__(name_dict):
    twitter_handles_no__ = []
    twitter = twitter_handles(name_dict)
    for handle in twitter: 
        handle_no = handle.replace("@", "")
        twitter_handles_no__.append(handle_no)
    return twitter_handles_no__

def searche_list(name_dict): 
    searche_list = [name_list(name_dict), twitter_handles(name_dict), twitter_handles_no__(name_dict), Influencers(name_dict)]
    searches = ["Collection Name","Twitter Handles","Twitter Handles (no @)","Influencers"]
    search_df = pd.DataFrame()
    for i, name in enumerate(searche_list):
        search_df[searches[i]] = name
    return search_df

def main_list(name_dict): 

    search_df = searche_list(name_dict)

    main_list = []
    for i in range(len(search_df)):  
        new_list = []
        for j in range(len(search_df.columns)):
            new_list.append(search_df.iloc[i,:][j])
            cleanedList = [x for x in new_list if str(x) != 'nan']
        main_list.append(cleanedList)
    return main_list
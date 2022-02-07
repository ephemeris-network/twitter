import pandas as pd
import tweepy
import credentials as cred
import functions as ft 
import scraper_input_data as scrape
import os 

#Configure the API 
auth = tweepy.AppAuthHandler(cred.consumer_key, cred.consumer_secret)
api = tweepy.API(auth, wait_on_rate_limit=True)    # set wait_on_rate_limit =True; as twitter may block you from querying if it finds you exceeding some limits

#Set the number of tweets
item_num = 50 

#Define the columns used in the datafram 
columns = ["Collection", "Tweets Scraped", "Polarity","Words Used"]

# Columns = ["ID", "NAME", "TIME", "POLARITY", "TWEET","HANDLE"]

# Make an empty dataframe, using the columns above 
df_all_collection = pd.DataFrame(columns = columns)

# A list of lists containing all text that will be used to search twitter for each collection
text_list = ft.main_list(scrape.name_dict)

# Loop through the program, collection by collection
# This is where the magic happens
for i in range(len(text_list)): 
    
    # Defining "Topics" as the list of words to search for (comes from scraper_input_data)
    Topics = text_list[i]
    tweets = []

    for i, topic in enumerate(Topics): 
        tweets.append(tweepy.Cursor(api.search_tweets, (topic),lang="en").items(item_num))

    count = 0
    for i, topic in enumerate(Topics):
        tweet_analyzer = ft.TweetAnalyzer(pd.DataFrame(columns=['Polarity - {}'.format(Topics[i])]))
        df = tweet_analyzer.tweets_to_data_frame(tweets[i], topic)
        if count == 0: 
            df_main = df
            count += 1
        else: 
            df_main = pd.concat([df_main, df], axis=1)

    Volume = []
    Polarity = []
    for i, topic in enumerate(Topics): 
        Volume.append(df_main['Polarity - {}'.format(topic)].shape[0])
        Polarity.append(df_main['Polarity - {}'.format(topic)].mean())

    zippedList =list(zip(Topics, Volume, Polarity))

    df_11 = pd.DataFrame(zippedList,columns=('Topics','Volume','Polarity'))
    df_sort = df_11.sort_values(by="Volume", ascending=False ,kind="mergesort")

    collection_name = Topics[0].replace(" ", "_")
    outname = 'Sentiment_{}.csv'.format(collection_name)
    outdir = './Twitter_Data/Individual_Collection_Data'

    if not os.path.exists(outdir):
        os.mkdir(outdir)
    fullname = os.path.join(outdir, outname)    

    df_sort.to_csv(fullname,index=False)
    
    Collection_name = Topics[0]
    collection_polarity = df_sort["Polarity"].mean()
    new_row = {'Collection':Collection_name, 'Tweets Scraped':item_num, 'Polarity': collection_polarity, "Words Used":Topics}
    df_all_collection = df_all_collection.append(new_row, ignore_index=True) 

df_all_collection.to_csv('./Twitter_Data/All_Collections_Data/Sentiment_All_Collections.csv',index=False)
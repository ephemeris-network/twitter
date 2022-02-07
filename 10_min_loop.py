import pandas as pd
import tweepy
import credentials as cred
import scraper_input_data as input_data
from textblob import TextBlob
import pandas as pd
from datetime import datetime, timedelta
import psycopg2
import postgress_config as config 


"""Things needing to be added: 
    - (1) Make code run in a infinite loop
    - (2) integrate a timer to wait 10 minuted between each loop
    - (3) add start and end date and time to the twitter search
    - (4) get data to dataframe working (currently not liking the variable types) 
    - (5) add sentiment analysis to the text analysis"""

"""defining the number of tweets to scrape"""
item_num = 1 

"""defining the time period over which to scrape"""
wait_time = 10 

"""Configure the API"""
auth = tweepy.AppAuthHandler(cred.consumer_key, cred.consumer_secret)
api = tweepy.API(auth, wait_on_rate_limit=True)    # set wait_on_rate_limit =True; as twitter may block you from querying if it finds you exceeding some limits

"""Used to define the tweet, it's polarity and time"""
class TweetAnalyzer():
    def __init__(self, df):
        self.df = df

    def tweet_info(self, tweets):
        for tweet in tweets:
            polarity = (TextBlob(tweet.text)).sentiment.polarity
            time = tweet.created_at.utcnow()
            tweet = tweet.text
            #ADD SENTIMENT VARIABLE
        return polarity, time, tweet 

"""Function to creat lists of twitter handles, collection names and 
    collection IDs from the source data (scraper_input_data.py)"""
def twitter_hand(name_dict):
    name_list = []
    for key in name_dict.keys():
        name_list.append(key)
    twitter_handles = []
    for i, name in enumerate(name_list):
        twitter_handles.append(name_dict[name]["Twitter"])
    id = []
    for i, name in enumerate(name_list):
        id.append(name_dict[name]["ID"])
    return twitter_handles, name_list, id

"""Creating lists of handles, names and collection IDs from the 
    source data (scraper_input_data.py)"""
handles, name_list, ids  = twitter_hand(input_data.name_dict) 

conn = None 
cur = None 

try: 
    """Connecting to the database using cedentials in config (postgress_config.py)"""
    conn = psycopg2.connect(
        host  = config.hostname,
        dbname = config.database,
        user = config.username,
        password = config.pwd,
        port = config.port_id)

    curr = conn.cursor()

    """Begining of infinite loop"""
    # While True loop starts here 

    """Defining the start and end time of each scrape"""
    EndTime = str(datetime.utcnow())+"Z"
    StartTime =  str(datetime.utcnow() - timedelta(minutes=wait_time))+"Z"

    for i, handle in enumerate(handles):
        
        tweets = []

        """This will be the main function that grams all tweets about a collection between a start and end date/time"""
        # tweets.append(tweepy.Cursor(api.search_30_day(label = auth, query = str(handle), formDate = StartTime, endDate = EndTime)))

        """Interim function until between dates can be defined, i.e. until advanced API is bought"""
        tweets.append(tweepy.Cursor(api.search_tweets, (handle),lang="en").items(item_num))

        for tweet in tweets:
            
            """Defining the variables to be inserted into the table"""
            tweet_analyzer = TweetAnalyzer(pd.DataFrame(columns=['Polarity - {}'.format(name_list[i])]))
            polarity, time, text  =  tweet_analyzer.tweet_info(tweet)
            polarity, time, text  = str(polarity), time ,str(text) 
            id = int(ids[i])
            name = str(name_list[i])
            twitter_handle = str(handle)

            """Me trying to get the query to work (not currently working)"""
            # polarity = polarity.replace(" ", "_")
            # time = time.replace("-", "")
            # tweet = tweet.replace(" ", "_") 
            # name = name.replace(" ", "_")

            """Defining and executing the SQL queery"""
            # query = "INSERT INTO public.Twitter_PC VALUES ({}, {}, {}, {}, {}, {});". format(id, name, str(time), text, polarity, twitter_handle) 
            # curr.execute(query)
           
            query = "INSERT INTO public.Twitter_PC VALUES (%s,%s,%s,%s,%s,%s);"
            curr.execute(query,(id, name, str(time), polarity, text, twitter_handle))
            
            conn.commit()

            # """Testing to see what comes out"""
            # print()

            """End of infinite loop"""
            # While True loop ends here 

#Error if credentials are wrong 
except Exception as error: 
    print(error)

#Close curser and connection
finally: 
    if cur is not None:
        cur.close()
    if conn is not None:
        conn.close()


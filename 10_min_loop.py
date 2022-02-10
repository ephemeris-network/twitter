import pandas as pd
import scraper_input_data as input_data
import pandas as pd
from datetime import datetime, timedelta
import psycopg2
import postgress_config as config 
import functions as ft 
import time 
import numpy as np
import table_names as table

"""the database used"""
database = table.test_ten_min

"""defining the time period over which to scrape"""
filename = './scrape_data.json'

"""time period over which to scrape"""
wait_time = 600

"""Creating lists of handles, names and collection IDs from the 
    source data (scraper_input_data.py)"""
handles, name_list, ids  = ft.input_lists(input_data.name_dict) 

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
    while 1: 
        Start  = time.time()
        for i, handle in enumerate(handles):
            
            """the search term"""
            search_term = handle

            """the start time of each scrape"""
            search_from =  str((datetime.utcnow() - timedelta(days=1)).date()) 

            """running the scrape using a command line prompt"""
            ft.scraper(search_from, search_term, filename)

            #read in the dataframe 
            df = pd.read_json(filename, lines=True)

            #sorting the time 
            df['date'] = pd.to_datetime(df.date).dt.tz_localize(None)
            pd.to_datetime(df['date'])

            #defining the last 10 minutes
            now = np.datetime64(datetime.utcnow())  
            ten_minutes_ago = np.datetime64(datetime.utcnow()-timedelta(minutes=10))     

            #new dataframe of the last ten minutes 
            ten_min_df = df[(df['date'] > pd.Timestamp(str(ten_minutes_ago))) & (df['date'] < pd.Timestamp(str(now)))]

            #for each tweet in the last ten minutes
            for i in range(ten_min_df.shape[0]):
                
                """Defining the variables to be inserted into the table"""
                timestamp = ten_min_df["date"].iloc[i]
                text = ten_min_df["content"].iloc[i]
                polarity = ft.polarity(ten_min_df["content"].iloc[i])
                id = int(ids[i])
                name = str(name_list[i])
                twitter_handle = str(handle)

                """Defining and executing the SQL queery"""
                query = "INSERT INTO {database} VALUES (%s,%s,%s,%s,%s,%s);"
                curr.execute(query,(id, name, str(timestamp), polarity, text, twitter_handle))
                
                conn.commit()
                print((time.time()-Start))

        """Making code sleep, such that each loop (inc scrape and sleep) takes 10 minutes"""
        if wait_time > (time.time() - Start): 
            # time.sleep(wait_time - (time.time() - Start))
            time.sleep(((time.time() - Start)+5) - (time.time() - Start))

        else: 
            print("Error: code took longer than 10 minutes to run. Rewite the code or run it on different core's to ensure each loop through all collections take less than 10 minutes")
            break 

#Error if credentials are wrong 
except Exception as error: 
    print(error)

#Close curser and connection
finally: 
    if cur is not None:
        cur.close()
    if conn is not None:
        conn.close()


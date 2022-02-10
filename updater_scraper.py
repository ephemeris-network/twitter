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
filename = './update_data.json'

"""the scrape period"""
scrape_period = 7 #days

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

        Start = time.time()

        for i, handle in enumerate(handles):
            
            """the search term"""
            search_term = handle

            """Days to scrape back from"""
            days_back = (scrape_period * n)

            """the start and end time of each scrape"""
            search_from =  str((datetime.utcnow() - timedelta(days=scrape_period)).date())
            search_until = str((datetime.utcnow()+ timedelta(days=1)).date())

            """run the scrape using cli"""
            ft.scraper(search_from, search_term, filename)

            """read in the dataframe""" 
            df = pd.read_json(filename, lines=True)

            """sort time to a usable format""" 
            df['date'] = pd.to_datetime(df.date).dt.tz_localize(None)
            pd.to_datetime(df['date'])

            """sequentially enter each row of the dataframe into the sql database"""
            for i in range(df.shape[0]):
                
                """Defining the variables to be inserted into the database"""
                timestamp = df["date"].iloc[i]
                text = df["content"].iloc[i]
                polarity = ft.polarity(df["content"].iloc[i])
                id = int(ids[i])
                name = str(name_list[i])
                twitter_handle = str(handle)

                """defining the SQL queery"""
                query = "INSERT INTO {database} VALUES (%s,%s,%s,%s,%s,%s);"

                """executing the SQL queery"""
                curr.execute(query,(id, name, str(timestamp), polarity, text, twitter_handle))
                conn.commit()

            #just checking how long that loop took
            print((time.time()-Start))

        

#Error if credentials are wrong 
except Exception as error: 
    print(error)

#Close curser and connection
finally: 
    if cur is not None:
        cur.close()
    if conn is not None:
        conn.close()
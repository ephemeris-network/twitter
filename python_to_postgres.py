from sqlite3 import Cursor
import psycopg2
import postgress_config as config 

conn = None 
cur = None 

try: 
    conn = psycopg2.connect(
        host  = config.hostname,
        dbname = config.database,
        user = config.username,
        password = config.pwd,
        port = config.port_id)

    curr = conn.cursor()
    
    queery= "INSERT INTO public.Twitter_PC VALUES (123,'SMB', 1, 0.5, 'Tweet', 'HANDLED');"

    curr.execute(queery)

    conn.commit()

#Error if credentials are wrong 
except Exception as error: 
    print(error)

#Close curser and connection
finally: 
    if cur is not None:
        cur.close()
    if conn is not None:
        conn.close()

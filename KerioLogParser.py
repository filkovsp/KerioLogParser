# To add a new cell, type '# %%'
# To add a new markdown cell, type '# %% [markdown]'
#

# %% --- this is not used for log parsing process
# from IPython import get_ipython
# get_ipython().run_line_magic('matplotlib', 'inline')
# import matplotlib.pyplot as plt

# %% --- main imports:
from kerio.Connection import Connection
from kerio.Tools import *
import pandas as pd
import numpy as np
import datetime as dt
import os, re

# %% --- PostgreSQL functionality support:
import psycopg2
from psycopg2 import extras
dsn = """
    host = 192.168.1.10
    dbname = kerio
    user = postgres
    password = postgres
    port = 5432
"""
# Alternative way to connect:
# conn = psycopg2.connect(
#     host = "192.168.1.10",
#     database = "kerio",
#     user = "postgres",
#     password = "postgres",
#     port = 5432) # default port = 5432

conn = psycopg2.connect(dsn = dsn)
conn.autocommit = True

# %% --- get list of all files in ./log/ folder:
files = [f for f in os.listdir('./logs') 
         if re.match(r'\Aconnection\.[\d{4}\-d{2}\-\d{2}].*\.log', f)]

# %% --- get only one file name:
files = ["connection.test.log"]


# %% --- process all files and upload into db
for x in files:
    filePath = "./logs/{0}".format(x)
    df = pd.DataFrame()
    start_time = dt.datetime.now()
    
    print("{0} starting with {1}".format(
        TimeUtil.getDTString(t=start_time), filePath))
    
    with open(file = filePath, mode = "r", buffering = 1_000_000) as f:
        for line in f:
            keys, values = Connection.parse(line)
            df = df.append(pd.DataFrame(data=[values], columns=keys), ignore_index=True)

    # --- Set DataFrame.index to the unique "ID" field:
    # df.set_index("ID", inplace=True)
    # --- Replace noname user name with "NaN" value:
    # df["User"].mask(df["User"] == "", "NaN", inplace=True)

    # --- Replace empty DestinationHostName with their DestinationIp.
    # in some cases it looks like Kerio bug, 
    # where, for a bunch of lines with the same Destination IP
    # there is just missing Destination HostName.
    # Possibly more sophisticated replacement is required here...
    # df["DestinationHost"].mask(
    #     df["DestinationHost"] == "", 
    #     df["DestinationIp"], inplace=True)

    # --- finally, count the processing time and show it as formatted string:
    print("{0}{1:<5}it took {2} to read {3} lines".format(
        TimeUtil.getDTString(), " ",
        TimeUtil.getDuration(t=start_time),
        df.shape[0]
    ))


    if len(df) > 0:
        columns = ",".join( list(map(lambda x: str('"{0}"'.format(x)), list(df.columns))) )

        # create VALUES('%s', '%s",...) one '%s' per column
        values = "VALUES ({})" \
            .format(",".join(["%s" for _ in list(df.columns)]))

        # --- create INSERT INTO table (columns) VALUES('%s',...)
        insert_stmt = "INSERT INTO {} ({}) {} on conflict do nothing" \
            .format('public."Connection"', columns, values)
        
        start_time = dt.datetime.now()
        print("{0}{1:<5}uploading data to db".format(
            TimeUtil.getDTString(t=start_time), " "
        ))

        try:
            cur = conn.cursor()
            extras.execute_batch(cur, insert_stmt, df.values)
            # commit if autocommit wasn't set above:
            # conn.commit()
            cur.close()
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)
        finally:
            # print("{0} has been processed...".format(filePath))
            print("{0}{1:<5}it took {2} to upload everything to db".format(
                TimeUtil.getDTString(), " ",
                TimeUtil.getDuration(t=start_time)
            ))
print("All done!")

# %% --- read from db:
"""
    cur = conn.cursor()
    cur.execute('select * from "Connection" limit 5')
    rows = cur.fetchall()
    columns = list(map(lambda x: x[0], cur.description))
    df = pd.DataFrame(data = rows, columns = columns)
    cur.close()
    conn.close()
"""

# %% --- close connection:
if conn is not None:
    conn.close()
    print('Database connection closed.')
    
    
    
    


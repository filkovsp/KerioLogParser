from __future__ import absolute_import, print_function, annotations
from kerio.Connection import Connection
from kerio.Tools import *
import pandas as pd


if (__name__ == "__main__"):
    
    # filePath = './logs/connection.test.log'
    filePath = './logs/connection.test.log'
    df = pd.DataFrame()
    start_time = dt.datetime.now()
    
    print("{0} starting with the file {1}".format(
        TimeUtil.getDTString(t=start_time), filePath))
            
    with open(file = filePath, mode = "r", buffering = 1_000_000) as f:
        for line in f:
            keys, values = Connection.parse(line)
            df = df.append(pd.DataFrame(data=[values], columns=keys), ignore_index =True)
    
    # --- Set DataFrame.index to the unique "ID" field:
    # df.set_index("ID", inplace=True)
    
    # --- Replace noname user name with "NaN" value:
    df["User"].mask(df["User"] == "", "NaN", inplace=True)
    
    # --- Replace empty DestinationHostName with their DestinationIp
    df["DestinationHost"].mask(
        df["DestinationHost"] == "", 
        df["DestinationIp"], inplace=True)

    print("{0}{1:<5}it took {2} to read {3} lines".format(
        TimeUtil.getDTString(), " ",
        TimeUtil.getDuration(t=start_time),
        df.shape[0]
    ))
    
    print("")
    print("\n\ndf.head():\n", df.head())
    print("\n\ndf.tail():\n", df.tail())
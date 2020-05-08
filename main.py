from __future__ import absolute_import, print_function, annotations
from include.Connection import Connection
from include.Tools import *
import pandas as pd
import numpy as np


if (__name__ == "__main__"):    
    
    # filePath = './logs/connection.test.log'
    filePath = './logs/connection.20200505.log'
    df = pd.DataFrame()
            
    with open(file = filePath, mode = "r", buffering = 1_000_000) as f:
        for line in f:
            keys, values = Connection.parse(line)
            df = df.append(pd.DataFrame(data=[values], columns=keys), ignore_index =True)
    
    # --- Set DataFrame.index to the unique "ID" field:
    df.set_index("ID", inplace=True)
    # --- Replace noname user name with "NaN" value:
    df["User"].mask(df["User"] == "", "NaN", inplace=True)
    # --- Replace empty DestinationHostName with their DestinationIp
    df["Connection.DestinationHostName"].mask(df["Connection.DestinationHostName"] == "", df["Connection.DestinationIp"], inplace=True)

    
    print(df.head())
    print(df.tail())
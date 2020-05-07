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
    
    df.set_index("ID", inplace=True)
    # sns.barplot(x = "sex", y = "survived", data=titanic)
    # sns.barplot(x="User", y="Bytes.Total", data=df, orient="v")
    # plt.show()
    
    print(df.head())
    print(df.tail())
# %%
import pykerio # https://pypi.org/project/PyKerio/
import ssl
from kerio.Connection import Connection
import pandas as pd
import time

import logging
import threading
import concurrent.futures
import numpy as np

import psycopg2
from psycopg2 import extras

# %% --- class Logger:
class KerioLogger:
    
    countLines = 300
    
    def __init__(self):
        self.df = pd.DataFrame()
        self._lock = threading.Lock()

        self._api = pykerio.PyKerioControl(server='192.168.1.10', port = 4081)

        self._application = pykerio.structs.ApiApplication({
            'name': "Kerio-Parser",
            'vendor': "VVP",
            'version': "0.1"
        })

        self._session = pykerio.interfaces.Session(self._api)
        self._session.login('admin', 'kilviola', self._application)
    
    def getData(self, id):
        with self._lock:
            response = self._api.request_rpc(
                method='Logs.get', 
                params = {'logName' : "connection", "fromLine" : -1, "countLines" : KerioLogger.countLines }
            )

            for line in response.result["viewport"]:
                keys, values = Connection.parse(str(line["content"]))
                self.df = self.df.append(pd.DataFrame(data=[values], columns=keys), ignore_index =True)
            
            self.df.drop_duplicates(subset=["ID", "DATETIME"], inplace=True)
            # self.df.reset_index(inplace=True)
            
            logging.info("{0}: got {1} records total".format(id, self.df.shape[0]))

    def putData(self, id):
        with self._lock:
            if len(self.df) > 0:
                dsn = """
                    host = 192.168.1.10
                    dbname = kerio
                    user = postgres
                    password = postgres
                    port = 5432"""
                conn = psycopg2.connect(dsn = dsn)
                conn.autocommit = True
                                
                # --- define column names:
                columns = ", ".join( list(map(lambda x: str('"{0}"'.format(x)), list(self.df.columns))) )
                
                # --- create VALUES('%s', '%s",...) one '%s' per column
                values = "VALUES ({})" \
                    .format(", ".join(["%s" for _ in list(self.df.columns)]))
                
                # --- create INSERT INTO table (columns) VALUES('%s',...)
                insert_stmt = "INSERT INTO {} ({}) {} on conflict do nothing" \
                    .format('public."ConnectionTest"', columns, values)
            
                try:
                    cur = conn.cursor()
                    extras.execute_batch(cur, insert_stmt, self.df.values)
                    cur.close()
                except (Exception, psycopg2.DatabaseError) as ex:
                    logging.debug(ex)
                    return False
                finally:
                    logging.info("{0}: pushed data into db and cleaned the buffer".format(id))
                    if conn is not None:
                        conn.close()
                        logging.info('Database connection closed.')
                    
            self.df = pd.DataFrame()
            
    def logout(self):
        self._session.logout()
        # same as this one below:
        # api.request_rpc(method="Session.logout", params={})
        logging.info("Logged out from the Kerio API server")
        
# %% --- main app:
if __name__ == "__main__":
    
    logging.basicConfig(
        format = "%(asctime)s: %(message)s", 
        level = logging.DEBUG,
        datefmt = "%H:%M:%S")
    
    ssl._create_default_https_context = ssl._create_unverified_context
        # probably needs to be done this way:
        # https://gist.github.com/michaelrice/a6794a017e349fc65d01
        
    logger = KerioLogger()
    
    READINGS_CYCLES_NUMBER = 25 # number
    PAUSE_BETEWEEN_READINGS = 3  # seconds
            
    # i = 0
    while True:
        # ThreadPoolExecutor with only 1 thread available at a time:
        with concurrent.futures.ThreadPoolExecutor(max_workers = 1) as executor:
            for id in range(READINGS_CYCLES_NUMBER):
                executor.submit(logger.getData, id)
                executor.submit(time.sleep, PAUSE_BETEWEEN_READINGS)
                
            executor.submit(logger.putData, READINGS_CYCLES_NUMBER)
        # i += 1
    
    # worker.logout()

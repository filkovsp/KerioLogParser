"""
    this file is supposed to be ran in VSCode supporting Jupyter code cells.
    For more info visit:
    https://code.visualstudio.com/docs/python/jupyter-support-py
"""

# %% --- main imports:
from IPython import get_ipython
get_ipython().run_line_magic('matplotlib', 'inline')
import matplotlib.pyplot as plt

from include.Tools import *
import pandas as pd
import numpy as np
import datetime as dt
import os, re

# %% --- PostgreSQL functionality support:
import psycopg2
dsn = """
    host = 192.168.1.10
    dbname = kerio
    user = postgres
    password = postgres
    port = 5432
"""
conn = psycopg2.connect(dsn = dsn)
conn.autocommit = True

# %% --- get the data:
# df = pd.DataFrame()
logDate = '2020-05-08'
query = """
select * from "Connection" where "DATETIME"::date = %s
"""

cur = conn.cursor()
cur.execute(query = query, vars=[logDate])
rows = cur.fetchall()
columns = list(map(lambda x: x[0], cur.description))
df = pd.DataFrame(data = rows, columns = columns)

# cur.close()
# conn.close()


# %%
# --- All the available users from the log:
users = list(df.groupby("User").groups)

# --- check what we've just got:
users


# %%
# --- OPTIONAL transformation of "users" list:

# NaN --> are unregistered users
# users.remove("NaN")

# Home --> my home deivces, e.g.: TV, Alexa, etc.
# users.remove("Home")

# --- Or, other possible ways to do the same:
# users = [user for user in users if user != "NaN"]
users = ["Pavel", "Veronika", "Vicky"]

# --- check what we've just got:
users


# %%
# --- check particular user's taffic data:
# df.loc[df["User"] == "User1"].head(5)


# %%
# collect summary data for Pie Chart:
traffic_totals = []

# --- traffic summary for a custom list of users only:
for user in users:
    traffic_totals.append(df.loc[df["User"] == user] \
        .groupby(["User"]) \
        .agg(np.sum)["Bytes.Total"].values[0])

traffic_summary = pd.DataFrame({
    "User": users, 
    "Traffic.MB": list(map(
        lambda size: BiteSize.transform(int(size), "MB"), traffic_totals
    ))
})

# --- check what we've got:
traffic_summary

# # %% [markdown]
# # Summary report:

# %%
# --- Build a Pie Chart out of summary data:
plt.figure(figsize=(8,5))
plt.pie(x=traffic_summary["Traffic.MB"], labels=traffic_summary["User"], autopct="%.2f%%")
plt.title("Traffic Summary: {0}".format(logDate))
plt.show()

# %% [markdown]
# # TOP 10 web-sites statistics:

# %%
# --- if necessary apply additional transformation to "users" list.
# --- then:
# --- create subplots to place all graphs at the same figure:
fig, ax = plt.subplots(nrows=len(users), ncols=1, constrained_layout=True, sharex=False, sharey=False)
fig.set_size_inches(10, 15)
fig.suptitle("Top 10 sites per user on {0}\n".format(logDate), fontsize=16)

# --- iterate through uach reporting user and build their statistics:
size_unit = "MB"
for index, user in enumerate(users):    
    user_traffic_per_host  = pd.DataFrame({
        
        "DestinationHost": list(df.loc[df["User"] == user]\
            .groupby(["DestinationHost"]).groups),

        "{0}.Total".format(size_unit) : list(map(
            lambda size: BiteSize.transform(int(size), size_unit),
            list(df.loc[df["User"] == user]\
                .groupby(["DestinationHost"])\
                .agg(np.sum)["Bytes.Total"])
        ))
    })

    user_traffic_per_host \
        .sort_values(by="{0}.Total".format(size_unit), ascending=True) \
        .tail(10).set_index("DestinationHost") \
        .plot(kind="barh", ax=ax[index])
    
    ax[index].title.set_text("{0}'s traffic: ".format(user))
    # ax[index].get_yaxis().set_label_position("right")

plt.show()

# %% [markdown]
# ## Query some particular user's details if necessary:

# %%
# Set user name:
user_name = "Veronika"


# %%
# User traffic by User Name: 
user_traffic = df.query(
        '`User` == "' + user_name + '" \
        ')[[
# --- columns ----------------------------------------------------------------------
        "User", "DATETIME", 
        "SourceHost", "SourceIp",
        "DestinationHost", "DestinationIp", 
        "DestinationPort", "Protocol",
        "Bytes.Total"]]

# --- Additional filters that we can use in df.query():
# and `DestinationHost`.str.contains("apple.com") \
# and `DATETIME` > "2020-05-29 11:00:00" and `DATETIME` < "2020-05-29 15:30:00" \


# %%
# --- Alternatively: User's traffic by Source Host Name:
user_traffic = df.query(
        '`User` == "' + user_name + '" \
        and `DATETIME` > "2020-09-10 17:30:00" and `DATETIME` < "2020-09-10 18:59:00" \
        # and `DestinationHost`.str.contains("roblox")\
        ')[[
# --- columns ----------------------------------------------------------------------
        "User", "DATETIME", 
        "SourceHost", "SourceIp",
        "DestinationHost", "DestinationIp", 
        "DestinationPort", "Protocol",
        "Bytes.Total"]]


# %%
# user_traffic.query('`DestinationHost`.str.contains("akamaitech")').head()
user_traffic.query(
    '`DestinationHost`.str.contains("akamaitech") \
    and `DestinationHost`.str.contains("22-146-144")').head()
# user_traffic.head()


# %%
# Total Sum whole logged traffic in human-friendly format:
BiteSize.transform(int(user_traffic.groupby(["User"]).agg(np.sum)["Bytes.Total"].values[0]))

# %% [markdown]
# ### More custom filtering conditions:

# %%
# --- get unique DestinationIp arrdesses:
# user_traffic.query('`DestinationHost`.str.contains("akamai")').DestinationIp.unique()
#
# --- get list of internal IP adresses associated with the user:
# list(user_traffic.groupby(["SourceIp", "SourceHost"]).groups)
#
# --- min time when traffic has started:
print(user_traffic.query('`DestinationHost`.str.contains("akamaitech")').groupby(["User"]).agg(np.min)["DATETIME"])
# e.g. 8:11 am
# --- max time when traffic has stopped:
print(user_traffic.query('`DestinationHost`.str.contains("akamaitech")').groupby(["User"]).agg(np.max)["DATETIME"])
# e.g. 9:29 am

# %% [markdown]
# ### Draw Top-10 bar:

# %%
size_unit = "MB"
user_traffic_per_host  = pd.DataFrame({
    "DestinationHost": list(user_traffic\
        .groupby(["DestinationHost"]).groups),

    "{0}.Total".format(size_unit) : list(map(
        lambda size: BiteSize.transform(int(size), size_unit),
        list(user_traffic\
            .groupby(["DestinationHost"])\
            .agg(np.sum)["Bytes.Total"])
    ))
})

user_traffic_per_host.sort_values(by="{0}.Total".format(size_unit), ascending=True).tail(10).set_index("DestinationHost").plot(kind="barh")
    
plt.show()

# %% [markdown]
# ## More deep analysis:

# %%
# unique DestinationIp adresses:
pd.DataFrame({"DestinationIp": user_traffic.query('`DestinationIp`.str.contains("209.")').DestinationIp.unique()})


# %%
# unique DestinationHost adresses:
user_traffic.DestinationHost.fillna(value="", inplace=True)
pd.DataFrame({"DestinationHost": user_traffic.query('`DestinationHost`.str.contains("google.com")').DestinationHost.unique()})

#%% [markdown]
# ## User-traffic by Hour as Bar-Chart:

# %%
user_traffic.reset_index(level=0, inplace=True)
user_traffic.set_index("DATETIME", inplace=True)
user_traffic.head()


# %%
user_traffic_byhour = user_traffic.groupby([user_traffic.index.hour.rename('Hour')])['Bytes.Total'].sum().reset_index().set_index("Hour")
user_traffic_byhour


# %%
size_unit = "MB"
pd.DataFrame({
    "{0}.Total".format(size_unit) : list(map(
        lambda size: BiteSize.transform(int(size), size_unit),
        list(user_traffic_byhour["Bytes.Total"])
    ))
}, index = user_traffic_byhour.index)\
 .plot(kind="bar", figsize = (8, 6))

plt.xticks(rotation=50)
plt.title("{0}'s traffic by Hour: ".format(user_name))
plt.show()

# %% [markdown]
# ### Closer look at some shorter period of time:

# %%
# --- hours between 8:00 and 16:00
# user_traffic_byhour.index.name
user_traffic_byhour.loc[16:19]


# %%
size_unit = "MB"
pd.DataFrame({
    "{0}.Total".format(size_unit) : list(map(
        lambda size: BiteSize.transform(int(size), size_unit),
        list(user_traffic_byhour.loc[17:19]["Bytes.Total"])
    ))
}, index=user_traffic_byhour.loc[17:19].index) \
  .plot(kind="bar")

plt.title(user_name)
plt.show()


# %%




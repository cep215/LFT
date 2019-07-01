import os
import requests
import datetime
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import pandas as pd
import numpy as np
import time
import math
from twisted.internet import task, reactor
from lft.db_def import Aggregate, Kraken


def closest_time(timenow):
    q = int(timenow / 60)

    time1 = 60 * q

    if ((timenow * 60) > 0):
        time2 = (60 * (q + 1))
    else:
        time2 = (60 * (q - 1))

    if (abs(timenow - time1) < abs(timenow - time2)):
        return time1

    return time2

# # Get current time till last minute
def timestamp_now():
    timenow = time.time()
    return closest_time(timenow)




basedir = os.path.abspath(os.path.dirname(__file__))
engine = create_engine('sqlite:///' + os.path.join(basedir, 'data.db'), echo=True)

# # create a Session
Session = sessionmaker(bind=engine)
session = Session()

period_list = np.array([4320, 1440, 360, 180, 60, 30, 15, 5, 3, 1])


def get_pandas(db):
    query = session.query(db).order_by(db.time)
    df = pd.read_sql(query.statement, session.bind)
    return df

# df_aggregate = get_pandas(Aggregate)
# print(df_aggregate.loc[df_aggregate['time'] == '1560857220'])


def get_avg(period, start, df):
    avg = 0
    for i in range(period) :
        time = str(start + i*60)
        # row = session.query(df).filter_by(time=time).first()
        row = df.loc[df['time'] == time]
        high = row.high.tolist()[0]
        low = row.low.tolist()[0]
        mid = (high + low) / 2
        avg += mid

    return avg/period



def get_avg_price(period, timenow, df):
    p1 = math.ceil(period/24)
    p2 = math.ceil(2/3 * period/24)
    avg_1 = get_avg(p1, int(timenow) - period*60, df)
    avg_2 = get_avg(p2, int(timenow) - p2*60, df)
    return  avg_1, avg_2


def max_price (period, timenow, df):
    max = 0
    start = int(timenow) - period * 60
    for i in range(period) :
        time = str(start + i*60)
        # row = session.query(df).filter_by(time=time).first()
        row = df.loc[df['time'] == time]
        high = row.high.tolist()[0]
        if(high > max):
            max = high
    return max

def min_price (period, timenow, df):
    start = int(timenow) - period * 60
    time = str(start)
    # row = session.query(df).filter_by(time=time).first()
    row = df.loc[df['time'] == time]
    low = row.low.tolist()[0]
    min = low
    for i in range(period) :
        time = str(start + i*60)
        # row = session.query(df).filter_by(time=time).first()
        row = df.loc[df['time'] == time]
        low = row.low.tolist()[0]
        if(low < min):
            min = low
    return min


def log_ret (period, timenow, df):
    avg_1, avg_2 = get_avg_price(period, timenow, df)
    return math.log(avg_2/avg_1)



def true_range (period, timenow, df):
    max = max_price(period, timenow, df)
    min = min_price(period, timenow, df)
    return (max - min)/ (max + min)



def get_df_until (df, timestamp):
    for index, row in df.iterrows():
        if int(row['time']) >= int(timestamp) :
            df.drop(index, inplace=True)
    return df






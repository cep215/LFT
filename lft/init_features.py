import os
import requests
import datetime
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import pandas as pd
import numpy as np
import time
import math
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

period = np.array([4320, 1440, 360, 180, 60, 30, 15, 5, 3, 1])


def get_btc_aggregate():
    query = session.query(Aggregate).order_by(Aggregate.time)
    df_aggregate = pd.read_sql(query.statement, session.bind)
    return df_aggregate

def get_btc_kraken():
    query = session.query(Kraken).order_by(Kraken.time)
    df_aggregate = pd.read_sql(query.statement, session.bind)
    return df_aggregate


def get_avg(period, start, db):
    avg = 0
    for i in range(period) :
        time = str(start + i*60)
        row = session.query(db).filter_by(time=time).first()
        high = row.high
        low = row.low
        mid = (high + low) / 2
        avg += mid

    return avg/period


def get_avg_price(period, timenow, db):
    p1 = math.ceil(period/24)
    p2 = math.ceil(2/3 * period/24)
    avg_1 = get_avg(p1, timenow - period*60, db)
    avg_2 = get_avg(p2, timenow - p2*60, db)
    return  avg_1, avg_2

def max_price (period, timenow, db):
    max = 0
    start = timenow - period * 60
    for i in range(period) :
        time = str(start + i*60)
        row = session.query(db).filter_by(time=time).first()
        high = row.high
        if(high > max):
            max = high
    return max

def min_price (period, timenow, db):
    start = timenow - period * 60
    time = str(start)
    row = session.query(db).filter_by(time=time).first()
    low = row.low
    min = low
    for i in range(period) :
        time = str(start + i*60)
        row = session.query(db).filter_by(time=time).first()
        low = row.low
        if(low < min):
            min = low
    return min


def feature_1 (period, timenow, db):
    avg_1, avg_2 = get_avg_price(60, timenow, db)
    return math.log(avg_2/avg_1)


def feature_2 (period, timenow, db):
    max = max_price(period, timenow, db)
    min = min_price(period, timenow, db)
    # return min, max
    return (max - min)/ (max + min)



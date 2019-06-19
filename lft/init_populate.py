import os
import csv
import requests
import datetime
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import pandas as pd

from db_def import Aggregate, Kraken

engine = create_engine('sqlite:///data.db', echo=True)

# create a Session
Session = sessionmaker(bind=engine)
session = Session()

# from collections import deque

# def tail(filepath):
#
#     try:
#         filepath.is_file
#         fp = str(filepath)
#     except AttributeError:
#         fp = filepath
#
#     with open(fp, "rb") as f:
#         size = os.stat(fp).st_size
#         start_pos = 0 if size - 1 < 0 else size - 1
#
#         if start_pos != 0:
#             f.seek(start_pos)
#             char = f.read(1)
#
#             if char == b"\n":
#                 start_pos -= 1
#                 f.seek(start_pos)
#
#             if start_pos == 0:
#                 f.seek(start_pos)
#             else:
#                 char = ""
#
#                 for pos in range(start_pos, -1, -1):
#                     f.seek(pos)
#
#                     char = f.read(1)
#
#                     if char == b"\n":
#                         break
#
#         return f.readline()

def insert_df_aggregate(df):


    # Create objects
    for index, row in df.iterrows():
        # access data using column names
        if(session.query(Aggregate.id).filter_by(time=row['time']).scalar()
                is None):
            agg = Aggregate(row['time'], row['close'], row['high'], row['low'],
                            row['open'], row['volumefrom'], row['volumeto'])
            session.add(agg)

    # commit the record the database
    session.commit()


def insert_df_kraken(df):


    # Create objects
    for index, row in df.iterrows():
        # access data using column names
        if (session.query(Kraken.id).filter_by(time=row['time']).scalar()
                is None):
            krk = Kraken(row['time'], row['close'], row['high'], row['low'],
                         row['open'], row['volumefrom'], row['volumeto'])
            session.add(krk)

    # commit the record the database
    session.commit()

# https://min-api.cryptocompare.com/data/histominute?fsym=BTC&tsym=USD&limit=2000&toTs = 1560791644

def minute_price_historical(symbol, comparison_symbol, timestamp, exchange):
    url = 'https://min-api.cryptocompare.com/data/histominute?fsym={}&tsym={' \
          '}&limit=2000'\
            .format(symbol.upper(), comparison_symbol.upper())
    if timestamp:
        url += '&toTs={}'.format(timestamp)
    if exchange:
        url += '&e={}'.format(exchange)
    page = requests.get(url)
    data = page.json()['Data']
    df = pd.DataFrame(data)
    if not df.empty:
        df['timestamp'] = [datetime.datetime.fromtimestamp(d) for d in df.time]
        return df
    else:
        return None


def get_last_aggregate():
    obj = session.query(Aggregate).order_by(Aggregate.time.desc()).first()
    return obj.time

def get_first_aggregate():
    obj = session.query(Aggregate).order_by(Aggregate.time).first()
    return obj.time

def populate():
    # df_aggregate = minute_price_historical('BTC', 'USD', '', '')
    # df_kraken = minute_price_historical('BTC', 'USD', '', 'kraken')
    # insert_df_aggregate(df_aggregate)
    # insert_df_kraken(df_kraken)
    time = ''
    # print(time)
    while(True):
        df_aggregate = minute_price_historical('BTC', 'USD', time, '')
        if (df_aggregate is None):
            break;
        df_kraken = minute_price_historical('BTC', 'USD', time, 'kraken')
        if(df_kraken is None):
            break;
        insert_df_aggregate(df_aggregate)
        insert_df_kraken(df_kraken)
        time = get_first_aggregate()
    print(time)


populate()

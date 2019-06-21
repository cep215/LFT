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

def minute_price_historical(symbol, comparison_symbol, exchange):
    url = 'https://min-api.cryptocompare.com/data/histominute?fsym={}&tsym={' \
          '}'\
            .format(symbol.upper(), comparison_symbol.upper())
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

def insert_df_aggregate(df):


    # Create objects
    for index, row in df.iterrows():
        # access data using column names]
        item = session.query(Aggregate).filter_by(time=row['time']).first()
        if (item is None):
        # if(session.query(Aggregate.id).filter_by(time=row['time']).scalar()
        #         is None):
            agg = Aggregate(row['time'], row['close'], row['high'], row['low'],
                            row['open'], row['volumefrom'], row['volumeto'])
            session.add(agg)
        elif (item.close is 0 or item.high is 0 or item.low is 0 or item.open is
        0 or item.volumefrom is 0 or item.volumeto is 0):
            item.close = row['close']
            item.high = row['high']
            item.low = row['low']
            item.open = row['open']
            item.volumefrom = row['volumefrom']
            item.volumeto = row['volumeto']

    # commit the record the database
    session.commit()


def insert_df_kraken(df):


    # Create objects
    for index, row in df.iterrows():
        # access data using column names
        item = session.query(Kraken).filter_by(time=row['time']).first()
        if (item is None):
        # if (session.query(Kraken.id).filter_by(time=row['time']).scalar()
        #         is None):
            krk = Kraken(row['time'], row['close'], row['high'], row['low'],
                         row['open'], row['volumefrom'], row['volumeto'])
            session.add(krk)
        elif (item.close is 0 or item.high is 0 or item.low is 0 or item.open is
        0 or item.volumefrom is 0 or item.volumeto is 0):
            item.close = row['close']
            item.high = row['high']
            item.low = row['low']
            item.open = row['open']
            item.volumefrom = row['volumefrom']
            item.volumeto = row['volumeto']


    # commit the record the database
    session.commit()



def populate():
    df_aggregate = minute_price_historical('BTC', 'USD', '')
    df_kraken = minute_price_historical('BTC', 'USD', 'kraken')
    insert_df_aggregate(df_aggregate)
    insert_df_kraken(df_kraken)

populate()

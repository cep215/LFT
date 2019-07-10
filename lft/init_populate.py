import os
import requests
import datetime
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import pandas as pd

from db_def import Aggregate, Kraken

basedir = os.path.abspath(os.path.dirname(__file__))
engine = create_engine('sqlite:///' + os.path.join(basedir, 'data.db'), echo=True)

# create a SQLAlchemy ORM Session
Session = sessionmaker(bind=engine)
session = Session()

def insert_df_aggregate(df):


    # Create objects
    for index, row in df.iterrows():
        # access data using column names]
        item = session.query(Aggregate).filter_by(time=row['time']).first()
        if (item is None):

            agg = Aggregate(row['time'], row['close'], row['high'], row['low'],
                            row['open'], row['volumefrom'], row['volumeto'])
            session.add(agg)


    # commit the record to the database
    session.commit()


def insert_df_kraken(df):


    # Create objects
    for index, row in df.iterrows():
        # access data using column names
        item = session.query(Kraken).filter_by(time=row['time']).first()
        if (item is None):
            krk = Kraken(row['time'], row['close'], row['high'], row['low'],
                         row['open'], row['volumefrom'], row['volumeto'])
            session.add(krk)

    # commit the record to the database
    session.commit()



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
    time = ''
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


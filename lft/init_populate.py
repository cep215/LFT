import os
import requests
import datetime
import pandas as pd
from db_def import Aggregate, Binance, engine
from db_def import session, Session


def insert_df_aggregate(df, from_currency, to_currency):


    # Create objects
    for index, row in df.iterrows():
        # access data using column names]
        item = session.query(Aggregate).filter_by(from_currency=from_currency).filter_by(to_currency=to_currency).filter_by(time=row['time']).first()
        if (item is None):

            agg = Aggregate(row['time'], row['close'], row['high'], row['low'],
                            row['open'], row['volumefrom'], row['volumeto'], from_currency=from_currency, to_currency=to_currency)
            session.add(agg)
    # commit the record to the database
    session.commit()



def minute_price_historical(symbol, comparison_symbol, timestamp, exchange):
    # If possible returns a Dataframe for the last 2000 minutes
    url = 'https://min-api.cryptocompare.com/data/histominute?fsym={}&tsym={}&limit=2000'\
            .format(symbol.upper(), comparison_symbol.upper())
    if timestamp:
        url += '&toTs={}'.format(timestamp)
    if exchange:
        url += '&e={}'.format(exchange)
    page = requests.get(url)
    data = page.json()['Data']
    df   = pd.DataFrame(data)
    if not df.empty:
        df['timestamp'] = [datetime.datetime.fromtimestamp(d) for d in df.time]
        return df
    else:
        return None



def get_last_aggregate():
    # Return the time of the oldest element
    obj = session.query(Aggregate).order_by(Aggregate.time.desc()).first()
    return obj.time

def get_first_aggregate():
    #Return the time of the oldest element
    obj = session.query(Aggregate).order_by(Aggregate.time).first()
    return obj.time

def populate():
    time = ''
    while(True):
        # Aggregate
        df_aggregate_btc = minute_price_historical('BTC', 'USD', time, '')
        if (df_aggregate_btc is None):
            break;

        insert_df_aggregate(df_aggregate_btc, 'BTC', 'USD')


        time = get_first_aggregate()
    print(time)


populate()


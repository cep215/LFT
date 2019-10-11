import os
import requests
import datetime
import pandas as pd
from db_def import Aggregate, Binance, engine
from db_def import session, Session



def minute_price_historical(symbol, comparison_symbol, exchange):
    # If possible returns a Dataframe for the last 5 minutes
    url = 'https://min-api.cryptocompare.com/data/histominute?fsym={}&tsym={' \
          '}&limit=2000'\
            .format(symbol.upper(), comparison_symbol.upper())
    if exchange:
        url += '&e={}'.format(exchange)
    page = requests.get(url)
    data = page.json()['Data']
    df   = pd.DataFrame(data)
    if not df.empty:
        df['timestamp'] = [datetime.datetime.fromtimestamp(d) for d in df['time']]
        return df
    else:
        return None


def insert_df_aggregate(df, from_currency, to_currency):
    for index, row in df.iterrows():
        # access data using column names]
        item = session.query(Aggregate).filter_by(from_currency=from_currency).filter_by(to_currency=to_currency).filter_by(time=row['time']).first()
        if (item is None):

            agg = Aggregate(row['time'], row['close'], row['high'], row['low'],
                            row['open'], row['volumefrom'], row['volumeto'], from_currency=from_currency, to_currency=to_currency)
            session.add(agg)
        # check back in time to see if volume of transactions increased
        else:
            item.close      = row['close']
            item.high       = row['high']
            item.low        = row['low']
            item.open       = row['open']
            item.volumefrom = row['volumefrom']
            item.volumeto   = row['volumeto']


    # commit the record to the database
    session.commit()


def populate():

    df_aggregate_btc = minute_price_historical('BTC', 'USD', '')
    insert_df_aggregate(df_aggregate_btc, 'BTC', 'USD')


populate()

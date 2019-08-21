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


def insert_df_kraken(df, from_currency, to_currency):


    # Create objects
    for index, row in df.iterrows():
        # access data using column names
        item = session.query(Kraken).filter_by(from_currency=from_currency).filter_by(to_currency=to_currency).filter_by(time=row['time']).first()
        if (item is None):
            krk = Kraken(row['time'], row['close'], row['high'], row['low'],
                         row['open'], row['volumefrom'], row['volumeto'], from_currency=from_currency, to_currency=to_currency)
            session.add(krk)

    # commit the record to the database
    session.commit()

# def insert_df_bitmex(df, from_currency, to_currency):
#
#
#     # Create objects
#     for index, row in df.iterrows():
#         # access data using column names
#         item = session.query(Bitmex).filter_by(time=row['time']).first()
#         if (item is None):
#             btx = Bitmex(row['time'], row['close'], row['high'], row['low'],
#                          row['open'], row['volumefrom'], row['volumeto'], from_currency=from_currency, to_currency=to_currency)
#             session.add(btx)
#
#     # commit the record to the database
#     session.commit()



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
        # Aggregate
        df_aggregate_btc = minute_price_historical('BTC', 'USD', time, '')
        if (df_aggregate_btc is None):
            break;

        df_aggregate_eth = minute_price_historical('ETH', 'USD', time, '')

        df_aggregate_xrp = minute_price_historical('XRP', 'USD', time, '')

        df_aggregate_ltc = minute_price_historical('LTC', 'USD', time, '')

        df_aggregate_bch = minute_price_historical('BCH', 'USD', time, '')

        df_aggregate_bnb = minute_price_historical('BNB', 'USD', time, '')

        df_aggregate_usdt = minute_price_historical('USDT', 'USD', time, '')

        df_aggregate_eos = minute_price_historical('EOS', 'USD', time, '')

        df_aggregate_bsv = minute_price_historical('BSV', 'USD', time, '')

        df_aggregate_xlm = minute_price_historical('XLM', 'USD', time, '')

        df_aggregate_ada = minute_price_historical('ADA', 'USD', time, '')

        df_aggregate_trx = minute_price_historical('TRX', 'USD', time, '')

        df_aggregate_xmr = minute_price_historical('XMR', 'USD', time, '')

        df_aggregate_leo = minute_price_historical('LEO', 'USD', time, '')

        df_aggregate_dash = minute_price_historical('DASH', 'USD', time, '')

        df_aggregate_btc_eth = minute_price_historical('BTC', 'ETH', time, '')

        df_aggregate_btc_xrp = minute_price_historical('BTC', 'XRP', time, '')

        df_aggregate_btc_ltc = minute_price_historical('BTC', 'LTC', time, '')

        df_aggregate_btc_bch = minute_price_historical('BTC', 'BCH', time, '')

        df_aggregate_eth_xrp = minute_price_historical('ETH', 'XRP', time, '')

        df_aggregate_eth_ltc = minute_price_historical('ETH', 'LTC', time, '')

        df_aggregate_eth_bch = minute_price_historical('ETH', 'BCH', time, '')

        df_aggregate_xrp_ltc = minute_price_historical('XRP', 'LTC', time, '')

        df_aggregate_xrp_bch = minute_price_historical('XRP', 'BCH', time, '')

        df_aggregate_ltc_bch = minute_price_historical('LTC', 'BCH', time, '')


        insert_df_aggregate(df_aggregate_btc, 'BTC', 'USD')
        insert_df_aggregate(df_aggregate_ada, 'ADA', 'USD')
        insert_df_aggregate(df_aggregate_bch, 'BCH', 'USD')
        insert_df_aggregate(df_aggregate_bnb, 'BNB', 'USD')
        insert_df_aggregate(df_aggregate_bsv, 'BSV', 'USD')
        insert_df_aggregate(df_aggregate_dash, 'DASH', 'USD')
        insert_df_aggregate(df_aggregate_eos, 'EOS', 'USD')
        insert_df_aggregate(df_aggregate_leo, 'LEO', 'USD')
        insert_df_aggregate(df_aggregate_eth, 'ETH', 'USD')
        insert_df_aggregate(df_aggregate_ltc, 'LTC', 'USD')
        insert_df_aggregate(df_aggregate_trx, 'TRX', 'USD')
        insert_df_aggregate(df_aggregate_usdt, 'USDT', 'USD')
        insert_df_aggregate(df_aggregate_xlm, 'XLM', 'USD')
        insert_df_aggregate(df_aggregate_xmr, 'XMR', 'USD')
        insert_df_aggregate(df_aggregate_xrp, 'XRP', 'USD')
        insert_df_aggregate(df_aggregate_btc_eth, 'BTC', 'ETH')
        insert_df_aggregate(df_aggregate_btc_xrp, 'BTC', 'XRP')
        insert_df_aggregate(df_aggregate_btc_ltc, 'BTC', 'LTC')
        insert_df_aggregate(df_aggregate_btc_bch, 'BTC', 'BCH')
        insert_df_aggregate(df_aggregate_eth_xrp, 'ETH', 'XRP')
        insert_df_aggregate(df_aggregate_eth_ltc, 'ETH', 'LTC')
        insert_df_aggregate(df_aggregate_eth_bch, 'ETH', 'BCH')
        insert_df_aggregate(df_aggregate_ltc_bch, 'LTC', 'BCH')
        insert_df_aggregate(df_aggregate_xrp_ltc, 'XRP', 'LTC')
        insert_df_aggregate(df_aggregate_xrp_bch, 'XRP', 'BCH')



        # Kraken
        df_kraken_btc = minute_price_historical('BTC', 'USD', time, 'kraken')
        if(df_kraken_btc is None):
            break;

        df_kraken_eth = minute_price_historical('ETH', 'USD', time, 'kraken')

        df_kraken_xrp = minute_price_historical('XRP', 'USD', time, 'kraken')

        df_kraken_ltc = minute_price_historical('LTC', 'USD', time, 'kraken')

        df_kraken_bch = minute_price_historical('BCH', 'USD', time, 'kraken')

        df_kraken_usdt = minute_price_historical('USDT', 'USD', time, 'kraken')

        df_kraken_eos = minute_price_historical('EOS', 'USD', time, 'kraken')

        df_kraken_bsv = minute_price_historical('BSV', 'USD', time, 'kraken')

        df_kraken_xlm = minute_price_historical('XLM', 'USD', time, 'kraken')

        df_kraken_ada = minute_price_historical('ADA', 'USD', time, 'kraken')

        df_kraken_xmr = minute_price_historical('XMR', 'USD', time, 'kraken')

        df_kraken_dash = minute_price_historical('DASH', 'USD', time, 'kraken')

        df_kraken_btc_ltc = minute_price_historical('BTC', 'LTC', time, 'kraken')



        insert_df_kraken(df_kraken_btc, 'BTC', 'USD')
        insert_df_kraken(df_kraken_ada, 'ADA', 'USD')
        insert_df_kraken(df_kraken_bch, 'BCH', 'USD')
        insert_df_kraken(df_kraken_bsv, 'BSV', 'USD')
        insert_df_kraken(df_kraken_dash, 'DASH', 'USD')
        insert_df_kraken(df_kraken_eos, 'EOS', 'USD')
        insert_df_kraken(df_kraken_eth, 'ETH', 'USD')
        insert_df_kraken(df_kraken_ltc, 'LTC', 'USD')
        insert_df_kraken(df_kraken_usdt, 'USDT', 'USD')
        insert_df_kraken(df_kraken_xlm, 'XLM', 'USD')
        insert_df_kraken(df_kraken_xmr, 'XMR', 'USD')
        insert_df_kraken(df_kraken_xrp, 'XRP', 'USD')
        insert_df_kraken(df_kraken_btc_ltc, 'BTC', 'LTC')

        time = get_first_aggregate()
    print(time)


populate()


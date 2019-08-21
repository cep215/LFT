import os
import csv
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


def minute_price_historical(symbol, comparison_symbol, exchange):
    url = 'https://min-api.cryptocompare.com/data/histominute?fsym={}&tsym={' \
          '}&limit=20'\
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
            item.close = row['close']
            item.high = row['high']
            item.low = row['low']
            item.open = row['open']
            item.volumefrom = row['volumefrom']
            item.volumeto = row['volumeto']


    # commit the record to the database
    session.commit()

    #
    # # Create objects
    # for index, row in df.iterrows():
    #     # access data using column names]
    #     item = session.query(Aggregate).filter_by(time=row['time']).first()
    #     if (item is None):
    #
    #         agg = Aggregate(row['time'], row['close'], row['high'], row['low'],
    #                         row['open'], row['volumefrom'], row['volumeto'])
    #         session.add(agg)
    #     # check back in time to see if volume of transactions increased
    #     else:
    #         item.close = row['close']
    #         item.high = row['high']
    #         item.low = row['low']
    #         item.open = row['open']
    #         item.volumefrom = row['volumefrom']
    #         item.volumeto = row['volumeto']
    #
    # # commit the record to the database
    # session.commit()


def insert_df_kraken(df, from_currency, to_currency):


    # Create objects
    for index, row in df.iterrows():
        # access data using column names
        item = session.query(Kraken).filter_by(from_currency=from_currency).filter_by(to_currency=to_currency).filter_by(time=row['time']).first()
        if (item is None):
            krk = Kraken(row['time'], row['close'], row['high'], row['low'],
                         row['open'], row['volumefrom'], row['volumeto'], from_currency=from_currency, to_currency=to_currency)
            session.add(krk)
        # check back in time to see if volume of transactions increased
        else:
            item.close = row['close']
            item.high = row['high']
            item.low = row['low']
            item.open = row['open']
            item.volumefrom = row['volumefrom']
            item.volumeto = row['volumeto']

    # commit the record to the database
    session.commit()

# def insert_df_kraken(df):
#
#
#     # Create objects
#     for index, row in df.iterrows():
#         # access data using column names
#         item = session.query(Kraken).filter_by(time=row['time']).first()
#         if (item is None):
#             krk = Kraken(row['time'], row['close'], row['high'], row['low'],
#                          row['open'], row['volumefrom'], row['volumeto'])
#             session.add(krk)
#         # check back in time to see if volume of transactions increased
#         else:
#             item.close = row['close']
#             item.high = row['high']
#             item.low = row['low']
#             item.open = row['open']
#             item.volumefrom = row['volumefrom']
#             item.volumeto = row['volumeto']
#
#
#     # commit the record to the database
#     session.commit()



def populate():

    df_aggregate_btc = minute_price_historical('BTC', 'USD', '')

    df_aggregate_eth = minute_price_historical('ETH', 'USD', '')

    df_aggregate_xrp = minute_price_historical('XRP', 'USD', '')

    df_aggregate_ltc = minute_price_historical('LTC', 'USD', '')

    df_aggregate_bch = minute_price_historical('BCH', 'USD', '')

    df_aggregate_bnb = minute_price_historical('BNB', 'USD', '')

    df_aggregate_usdt = minute_price_historical('USDT', 'USD', '')

    df_aggregate_eos = minute_price_historical('EOS', 'USD', '')

    df_aggregate_bsv = minute_price_historical('BSV', 'USD', '')

    df_aggregate_xlm = minute_price_historical('XLM', 'USD', '')

    df_aggregate_ada = minute_price_historical('ADA', 'USD', '')

    df_aggregate_trx = minute_price_historical('TRX', 'USD', '')

    df_aggregate_xmr = minute_price_historical('XMR', 'USD', '')

    df_aggregate_leo = minute_price_historical('LEO', 'USD', '')

    df_aggregate_dash = minute_price_historical('DASH', 'USD', '')

    df_aggregate_btc_eth = minute_price_historical('BTC', 'ETH', '')

    df_aggregate_btc_xrp = minute_price_historical('BTC', 'XRP', '')

    df_aggregate_btc_ltc = minute_price_historical('BTC', 'LTC', '')

    df_aggregate_btc_bch = minute_price_historical('BTC', 'BCH', '')

    df_aggregate_eth_xrp = minute_price_historical('ETH', 'XRP', '')

    df_aggregate_eth_ltc = minute_price_historical('ETH', 'LTC', '')

    df_aggregate_eth_bch = minute_price_historical('ETH', 'BCH', '')

    df_aggregate_xrp_ltc = minute_price_historical('XRP', 'LTC', '')

    df_aggregate_xrp_bch = minute_price_historical('XRP', 'BCH', '')

    df_aggregate_ltc_bch = minute_price_historical('LTC', 'BCH', '')

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

    df_kraken_btc = minute_price_historical('BTC', 'USD', 'kraken')

    df_kraken_eth = minute_price_historical('ETH', 'USD', 'kraken')

    df_kraken_xrp = minute_price_historical('XRP', 'USD', 'kraken')

    df_kraken_ltc = minute_price_historical('LTC', 'USD', 'kraken')

    df_kraken_bch = minute_price_historical('BCH', 'USD', 'kraken')

    df_kraken_usdt = minute_price_historical('USDT', 'USD', 'kraken')

    df_kraken_eos = minute_price_historical('EOS', 'USD', 'kraken')

    df_kraken_bsv = minute_price_historical('BSV', 'USD', 'kraken')

    df_kraken_xlm = minute_price_historical('XLM', 'USD', 'kraken')

    df_kraken_ada = minute_price_historical('ADA', 'USD', 'kraken')

    df_kraken_xmr = minute_price_historical('XMR', 'USD', 'kraken')

    df_kraken_dash = minute_price_historical('DASH', 'USD', 'kraken')

    df_kraken_btc_ltc = minute_price_historical('BTC', 'LTC', 'kraken')


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

    # df_aggregate = minute_price_historical('BTC', 'USD', '')
    # df_kraken = minute_price_historical('BTC', 'USD', 'kraken')
    # insert_df_aggregate(df_aggregate)
    # insert_df_kraken(df_kraken)

populate()

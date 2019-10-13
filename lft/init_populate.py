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

def insert_df_binance(df, from_currency, to_currency):


    # Create objects
    for index, row in df.iterrows():
        # access data using column names]
        item = session.query(Binance).filter_by(from_currency=from_currency).filter_by(to_currency=to_currency).filter_by(time=row['time']).first()
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

        df_aggregate_eth = minute_price_historical('ETH', 'USD',time,  '')
        df_aggregate_xrp = minute_price_historical('XRP', 'USD',time ,  '')
        df_aggregate_ltc = minute_price_historical('LTC', 'USD',time ,  '')
        df_aggregate_bch = minute_price_historical('BCH', 'USD',time ,  '')
        df_aggregate_eos = minute_price_historical('EOS', 'USD',time ,  '')
        df_aggregate_xlm = minute_price_historical('XLM', 'USD',time ,  '')
        df_aggregate_trx = minute_price_historical('TRX', 'USD',time ,  '')

        insert_df_aggregate(df_aggregate_btc, 'BTC', 'USD')
        insert_df_aggregate(df_aggregate_bch, 'BCH', 'USD')
        insert_df_aggregate(df_aggregate_eos, 'EOS', 'USD')
        insert_df_aggregate(df_aggregate_eth, 'ETH', 'USD')
        insert_df_aggregate(df_aggregate_ltc, 'LTC', 'USD')
        insert_df_aggregate(df_aggregate_trx, 'TRX', 'USD')
        insert_df_aggregate(df_aggregate_xlm, 'XLM', 'USD')
        insert_df_aggregate(df_aggregate_xrp, 'XRP', 'USD')

        df_aggregate_btc_usdt = minute_price_historical('BTC', 'USDT',time ,  '')
        if (df_aggregate_btc_usdt is None):
            break;

        df_aggregate_eth_usdt = minute_price_historical('ETH', 'USDT',time ,  '')
        df_aggregate_xrp_usdt = minute_price_historical('XRP', 'USDT',time ,  '')
        df_aggregate_ltc_usdt = minute_price_historical('LTC', 'USDT',time ,  '')
        df_aggregate_bch_usdt = minute_price_historical('BCH', 'USDT',time ,  '')
        df_aggregate_eos_usdt = minute_price_historical('EOS', 'USDT',time ,  '')
        df_aggregate_xlm_usdt = minute_price_historical('XLM', 'USDT',time ,  '')
        df_aggregate_trx_usdt = minute_price_historical('TRX', 'USDT',time ,  '')

        insert_df_aggregate(df_aggregate_btc_usdt, 'BTC', 'USDT')
        insert_df_aggregate(df_aggregate_bch_usdt, 'BCH', 'USDT')
        insert_df_aggregate(df_aggregate_eos_usdt, 'EOS', 'USDT')
        insert_df_aggregate(df_aggregate_eth_usdt, 'ETH', 'USDT')
        insert_df_aggregate(df_aggregate_ltc_usdt, 'LTC', 'USDT')
        insert_df_aggregate(df_aggregate_trx_usdt, 'TRX', 'USDT')
        insert_df_aggregate(df_aggregate_xlm_usdt, 'XLM', 'USDT')
        insert_df_aggregate(df_aggregate_xrp_usdt, 'XRP', 'USDT')

        df_binance_btc = minute_price_historical('BTC', 'USDT', 'binance')
        if (df_binance_btc is None):
            break;

        df_binance_eth = minute_price_historical('ETH', 'USDT', 'binance')
        df_binance_xrp = minute_price_historical('XRP', 'USDT', 'binance')
        df_binance_ltc = minute_price_historical('LTC', 'USDT', 'binance')
        df_binance_bch = minute_price_historical('BCH', 'USDT', 'binance')
        df_binance_eos = minute_price_historical('EOS', 'USDT', 'binance')
        df_binance_xlm = minute_price_historical('XLM', 'USDT', 'binance')
        df_binance_trx = minute_price_historical('TRX', 'USDT', 'binance')

        insert_df_binance(df_binance_btc, 'BTC', 'USDT')
        insert_df_binance(df_binance_bch, 'BCH', 'USDT')
        insert_df_binance(df_binance_eos, 'EOS', 'USDT')
        insert_df_binance(df_binance_eth, 'ETH', 'USDT')
        insert_df_binance(df_binance_ltc, 'LTC', 'USDT')
        insert_df_binance(df_binance_trx, 'TRX', 'USDT')
        insert_df_binance(df_binance_xlm, 'XLM', 'USDT')
        insert_df_binance(df_binance_xrp, 'XRP', 'USDT')


        time = get_first_aggregate()
    print(time)


populate()


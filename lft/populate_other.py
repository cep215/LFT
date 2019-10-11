import os
import requests
import datetime
import pandas as pd
from db_def import Aggregate, Binance, engine
from db_def import session, Session


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



def minute_price_historical(symbol, comparison_symbol, exchange):
    # If possible returns a Dataframe for the last 2000 minutes
    url = 'https://min-api.cryptocompare.com/data/histominute?fsym={}&tsym={}&limit=2000'\
            .format(symbol.upper(), comparison_symbol.upper())
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


def populate():
    #Aggregate USD
    df_aggregate_btc = minute_price_historical('BTC', 'USD', '')
    df_aggregate_eth = minute_price_historical('ETH', 'USD', '')
    df_aggregate_xrp = minute_price_historical('XRP', 'USD', '')
    df_aggregate_ltc = minute_price_historical('LTC', 'USD', '')
    df_aggregate_bch = minute_price_historical('BCH', 'USD', '')
    df_aggregate_eos = minute_price_historical('EOS', 'USD', '')
    df_aggregate_xlm = minute_price_historical('XLM', 'USD', '')
    df_aggregate_trx = minute_price_historical('TRX', 'USD', '')

    insert_df_aggregate(df_aggregate_btc, 'BTC', 'USD')
    insert_df_aggregate(df_aggregate_bch, 'BCH', 'USD')
    insert_df_aggregate(df_aggregate_eos, 'EOS', 'USD')
    insert_df_aggregate(df_aggregate_eth, 'ETH', 'USD')
    insert_df_aggregate(df_aggregate_ltc, 'LTC', 'USD')
    insert_df_aggregate(df_aggregate_trx, 'TRX', 'USD')
    insert_df_aggregate(df_aggregate_xlm, 'XLM', 'USD')
    insert_df_aggregate(df_aggregate_xrp, 'XRP', 'USD')

    #Aggregate USDT
    df_aggregate_btc = minute_price_historical('BTC', 'USDT', '')
    df_aggregate_eth = minute_price_historical('ETH', 'USDT', '')
    df_aggregate_xrp = minute_price_historical('XRP', 'USDT', '')
    df_aggregate_ltc = minute_price_historical('LTC', 'USDT', '')
    df_aggregate_bch = minute_price_historical('BCH', 'USDT', '')
    df_aggregate_eos = minute_price_historical('EOS', 'USDT', '')
    df_aggregate_xlm = minute_price_historical('XLM', 'USDT', '')
    df_aggregate_trx = minute_price_historical('TRX', 'USDT', '')

    insert_df_aggregate(df_aggregate_btc, 'BTC', 'USDT')
    insert_df_aggregate(df_aggregate_bch, 'BCH', 'USDT')
    insert_df_aggregate(df_aggregate_eos, 'EOS', 'USDT')
    insert_df_aggregate(df_aggregate_eth, 'ETH', 'USDT')
    insert_df_aggregate(df_aggregate_ltc, 'LTC', 'USDT')
    insert_df_aggregate(df_aggregate_trx, 'TRX', 'USDT')
    insert_df_aggregate(df_aggregate_xlm, 'XLM', 'USDT')
    insert_df_aggregate(df_aggregate_xrp, 'XRP', 'USDT')


    df_binance_btc = minute_price_historical('BTC', 'USDT', 'binance')
    df_binance_eth = minute_price_historical('ETH', 'USDT', 'binance')
    df_binance_xrp = minute_price_historical('XRP', 'USDT', 'binance')
    df_binance_ltc = minute_price_historical('LTC', 'USDT', 'binance')
    df_binance_bch = minute_price_historical('BCH', 'USDT', 'binance')
    df_binance_eos = minute_price_historical('EOS', 'USDT', 'binance')
    df_binance_xlm = minute_price_historical('XLM', 'USDT', 'binance')
    df_binance_trx = minute_price_historical('TRX', 'USDT', 'binance')

    insert_df_aggregate(df_binance_btc, 'BTC', 'USDT')
    insert_df_aggregate(df_binance_bch, 'BCH', 'USDT')
    insert_df_aggregate(df_binance_eos, 'EOS', 'USDT')
    insert_df_aggregate(df_binance_eth, 'ETH', 'USDT')
    insert_df_aggregate(df_binance_ltc, 'LTC', 'USDT')
    insert_df_aggregate(df_binance_trx, 'TRX', 'USDT')
    insert_df_aggregate(df_binance_xlm, 'XLM', 'USDT')
    insert_df_aggregate(df_binance_xrp, 'XRP', 'USDT')

populate()
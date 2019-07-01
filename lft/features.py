import time
import pandas as pd
import requests

from lft.init_features import *

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

def get_last_record(symbol, comparison_symbol, timestamp, exchange):
    url = 'https://min-api.cryptocompare.com/data/histominute?fsym={}&tsym={' \
          '}&limit=10' \
        .format(symbol.upper(), comparison_symbol.upper())
    if timestamp:
        url += '&toTs={}'.format(timestamp)
    if exchange:
        url += '&e={}'.format(exchange)
    page = requests.get(url)
    data = page.json()['Data']
    df = pd.DataFrame(data)

    if not df.empty:
        return df
    else:
        return None

def get_df_until (df, timestamp):
    for index, row in df.iterrows():
        if int(row['time']) >= int(timestamp) :
            df.drop(index, inplace=True)
    return df

def update_df(df, symbol, comparison_symbol, exchange):
    df_to_update = get_last_record(symbol, comparison_symbol, timestamp_now(), exchange)
    df_res = pd.concat([df, df_to_update]).drop_duplicates('time')
    return df_res
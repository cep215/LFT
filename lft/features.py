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

# Returns dataframe containing last 10 minutes of data (from cryptocompare api)
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

# Returns dataframe until given timestamp
def get_df_until (df, timestamp):
    for index, row in df.iterrows():
        if int(row['time']) >= int(timestamp) :
            df.drop(index, inplace=True)
    return df

# Returns old dataframe concatanated with new dataframe
def update_df(df, symbol, comparison_symbol, exchange):
    df_to_update = get_last_record(symbol, comparison_symbol, timestamp_now(), exchange)
    df_res = pd.concat([df, df_to_update]).drop_duplicates('time')
    return df_res


# def update():
#     df_aggregate = get_pandas(Aggregate)
#     for i in period_list:
#         df_aggregate['log_ret_' + str(i)] = 0.0
#         df_aggregate['true_range_' + str(i)] = 0.0
#
#     for index, row in df_aggregate.iterrows():
#         df = get_df_until(df_aggregate, row['time'])
#         if not df.empty:
#             for i in period_list:
#                 df_aggregate[df_aggregate['time'] == row['time']]['log_ret_'
#                                                                   + str(i)] = \
#                     log_ret(i, row['time'], df)
#
#     print(df_aggregate)

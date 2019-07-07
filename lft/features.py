import time
import pandas as pd
import requests
import numpy as np


from lft.init_features import create_past_df, log_ret
from lft.db_def import Aggregate, Kraken

period_list = np.array([4320, 1440, 360, 180, 60, 30, 15, 5, 3])

alpha = 0.01


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

def get_timestamp_now():
    timenow = time.time()
    return closest_time(timenow)

#########################################################
df = create_past_df(Aggregate).iloc[-4500:]
df = df.convert_objects(convert_numeric=True)
#########################################################


# Returns dataframe containing last 10 minutes of data (from cryptocompare api)
def get_last_record(symbol, comparison_symbol, timestamp, exchange):
    url = 'https://min-api.cryptocompare.com/data/histominute?fsym={}&tsym={}&limit=10'.format(symbol.upper(), comparison_symbol.upper())
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

def update_volume (df_to, df_from, index):
    last_row = df_to.iloc[index]
    if (last_row.volumeto == 0 or last_row.volumefrom == 0):
        row_to_update = df_from.loc[df_from['time'] == last_row.time]
        df_to.at[df_to.index[index], 'volumeto'] = row_to_update.volumeto
        df_to.at[df_to.index[index], 'volumefrom'] = row_to_update.volumefrom
    return df_to

# Returns old dataframe concatanated with new dataframe
def update_df(df, index, symbol, comparison_symbol, exchange):
    df_to_update = get_last_record(symbol, comparison_symbol, get_timestamp_now(), exchange)

    df = update_volume(df, df_to_update, index)

    df_res = pd.concat([df, df_to_update]).drop_duplicates(subset='time', keep = 'first').reset_index(drop=True)
    # print(df_res[['time', 'close', 'volumeto', 'ema_3']])

    return df_res



def update_df_features(df, index, symbol, comparison_symbol, exchange):
    df = update_df(df, index, symbol, comparison_symbol, exchange)


    ### Calculate log_ret
    for period in period_list:
        df['log_ret_'+str(period)].iloc[index] = 0
        df['log_ret_' + str(period)].iloc[index] = log_ret(period, df.index[index], df)



    ### Calculate feature true_range = (max-min)/(max+min) for different periods
    for period in period_list:
        # min = df['low'].rolling(window=period).min()
        min = np.min(df['low'].iloc[(-period + index) : index])
        # max = df['high'].rolling(window=period).max()
        max = np.max(df['low'].iloc[(-period + index) : index])
        df['true_range_' + str(period)].iloc[index] = (max - min) / (max + min)

    ### Calculate feature rel_volume_returns
    for period in period_list:
        # Calculate ema for different spans
        df['ema_volume_' + str(period)].iloc[index] = alpha * df['volumeto'].iloc[index] + (1-alpha) * df['ema_volume_'+ str(period)].iloc[index - 1]

        # Calculate returns on different periods
        # df['returns_' + str(period)].iloc[index] = df['close'].pct_change(periods=period)
        df['returns_' + str(period)].iloc[index] = (df['close'].iloc[index] - df['close'].iloc[-period + index]) / df['close'].iloc[-period + index]

        df['rel_volume_returns_' + str(period)].iloc[index] = df['volumeto'].iloc[index] / df['ema_volume_' + str(
            period)].iloc[index] * df['returns_' + str(period)].iloc[index]

    ### Calculate std_returns
    for period in period_list:
        # df['std_returns_' + str(period)].iloc[index] = df['returns_' + str(period)].rolling(window=period).std()
        df['std_returns_' + str(period)].iloc[index] = np.std(df['returns_' + str(period)].iloc[(-period + index) : index])

    ### Calculate Bollinger Bands
    for period in period_list:
        df['ema_close_' + str(period)].iloc[index] = alpha * df['close'].iloc[index] + (1-alpha) * df['ema_close_'+ str(period)].iloc[index - 1]
        # df['std_close_' + str(period)].iloc[index] = df['close'].rolling(window=period).std()
        df['std_close_' + str(period)].iloc[index] = np.std(df['close'].iloc[(-period + index) : index])

        df['lower_bb_' + str(period)].iloc[index] = df['ema_close_' + str(period)].iloc[index] - 2 * df['std_close_' + str(period)].iloc[index]
        df['upper_bb_' + str(period)].iloc[index] = df['ema_close_' + str(period)].iloc[index] + 2 * df['std_close_' + str(period)].iloc[index]

    return df

starttime=time.time()


for i in range (-10, 0):
    df = update_df_features(df, i, 'BTC', 'USD', '')

while True:
  df = update_df_features(df, -1, 'BTC', 'USD', '')
  df = df.iloc[1:]
  print(df[['time', 'close', 'volumeto', 'ema_volume_3']])
  time.sleep(60.0 - ((time.time() - starttime) % 60.0))






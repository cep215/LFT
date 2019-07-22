import math
import time
import pandas as pd
import requests
import numpy as np


from lft.init_features import create_past_df, log_ret, avg_ret, period_list, target_period_list
from lft.db_def import Aggregate, Kraken

pd.set_option('display.max_rows', 5000)




alpha = 0.01

# def get_avg(df, start, period):
#     avg = df['avg'].iloc[(start - period):start].mean(axis=0)
#     if (period != 0):
#         return avg
#     else:
#         return 1
#
# def log_ret(df, period, index):
#
#     p1 = math.ceil(period / 24)
#     p2 = math.ceil(2 / 3 * period / 24)
#     avg_1 = get_avg(df, index - period, p1)
#     avg_2 = get_avg(df, index - p2, p2)
#
#     if (avg_1 != 0):
#         return math.log(avg_2/avg_1)
#
# def avg_ret(df, period, index):
#
#     p1 = math.ceil(period / 24)
#     p2 = math.ceil(2 / 3 * period / 24)
#     avg_1 = get_avg(df, index - period, p1)
#     avg_2 = get_avg(df, index - p2, p2)
#     if (avg_1 != 0):
#         return (avg_2 - avg_1)/avg_1

#########################################################
df = create_past_df(Aggregate).iloc[-500:]
df = df.convert_objects(convert_numeric=True)
#########################################################


# Returns dataframe containing last 10 minutes of data (from cryptocompare api)
def get_last_record(symbol, comparison_symbol, exchange):
    url = 'https://min-api.cryptocompare.com/data/histominute?fsym={}&tsym={}&limit=5'.format(symbol.upper(), comparison_symbol.upper())
    if exchange:
        url += '&e={}'.format(exchange)
    page = requests.get(url)
    data = page.json()['Data']
    df = pd.DataFrame(data)
    df['time'] = df['time'].astype(np.int64)

    if not df.empty:
        return df
    else:
        return None



def update_pricevol (df, df_update):
    for index, row in df_update.iterrows():
        if not df.loc[df['time'] == row.time].empty:
            df["volumeto"].loc[df['time'] == row.time] = row.volumeto
            df["volumefrom"].loc[df['time'] == row.time] = row.volumefrom
            df["close"].loc[df['time'] == row.time] = row.close
            df["high"].loc[df['time'] == row.time] = row.high
            df["low"].loc[df['time'] == row.time]= row.low
            df["open"].loc[df['time'] == row.time] = row.open

        else:
            df = df.append(row, ignore_index = True)
    return df

# Returns old dataframe concatanated with new dataframe
def update_df(df, symbol, comparison_symbol, exchange):
    df_update = get_last_record(symbol, comparison_symbol, exchange)

    df = update_pricevol(df, df_update)

    # df_res = pd.concat([df, df_update]).drop_duplicates(subset='time', keep = 'first').reset_index(drop=True)

    return df



def update_df_features(df, symbol, comparison_symbol, exchange):
    df = update_df(df, symbol, comparison_symbol, exchange)


    index = df[df['avg'].isnull()].index

    ### Calculate target_price
    for period in target_period_list:
        min = df[::-1]['low'].rolling(window=period).min().shift()
        df['min_target_' + str(period)] = min
        max = df[::-1]['high'].rolling(window=period).max().shift()
        df['max_target_' + str(period)] = max


    for i in index:



        ### Calculate log_ret
        df['avg'].iloc[i] = (df['low'].iloc[i] + df['high'].iloc[i]) / 2
        for period in period_list:
            df['log_ret_' + str(period)].iloc[i] = log_ret(df, period, i)



        ### Calculate feature true_range = (max-min)/(max+min) for different periods
        for period in period_list:
            # min = df['low'].rolling(window=period).min()
            min = np.min(df['low'].iloc[(i - period) : i + 1])
            # max = df['high'].rolling(window=period).max()
            max = np.max(df['high'].iloc[(i - period) : i + 1])
            df['true_range_' + str(period)].iloc[i] = (max - min) / (max + min)

        ### Calculate feature rel_volume_returns
        for period in period_list:
            # Calculate ema for different spans
            df['ema_volume_' + str(period)].iloc[i] = alpha * df['volumeto'].iloc[i] + (1-alpha) * df['ema_volume_'+ str(period)].iloc[i - 1]

            # Calculate returns on different periods
            # df['returns_' + str(period)].iloc[i] = df['close'].pct_change(periods=period)
            df['returns_'+str(period)].iloc[i] = avg_ret(df, period, i)
            # df['returns_' + str(period)].iloc[i] = (df['close'].iloc[i] - df['close'].iloc[i - period]) / df['close'].iloc[i-period]

            df['rel_volume_returns_' + str(period)].iloc[i] = df['volumeto'].iloc[i] / df['ema_volume_' + str(
                period)].iloc[i] * df['returns_' + str(period)].iloc[i]

        ### Calculate std_returns
        for period in period_list:
            # df['std_returns_' + str(period)].iloc[index] = df['returns_' + str(period)].rolling(window=period).std()
            df['std_returns_' + str(period)].iloc[i] = np.std(df['returns_' + str(period)].iloc[(i - period) : i + 1])

        ### Calculate Bollinger Bands
        for period in period_list:
            df['ema_close_' + str(period)].iloc[i] = alpha * df['close'].iloc[i] + (1-alpha) * df['ema_close_'+ str(period)].iloc[i - 1]
            # df['std_close_' + str(period)].iloc[index] = df['close'].rolling(window=period).std()
            df['std_close_' + str(period)].iloc[i] = np.std(df['close'].iloc[(i - period) : i + 1])

            df['lower_bb_' + str(period)].iloc[i] = df['ema_close_' + str(period)].iloc[i] - 2 * df['std_close_' + str(period)].iloc[i]
            df['upper_bb_' + str(period)].iloc[i] = df['ema_close_' + str(period)].iloc[i] + 2 * df['std_close_' + str(period)].iloc[i]


    return df



starttime=time.time()

while True:
    df = update_df_features(df, 'BTC', 'USD', '')
    print("update df features index ", "\n",
          df[['time', 'open', 'close', 'high', 'low', 'volumeto', 'volumefrom',
              # 'ema_volume_5',
              # 'ema_close_5',
              # 'log_ret_5',
              # 'true_range_5',
              # 'rel_volume_returns_5',
              # 'std_close_5',
              # 'returns_5',
              # 'std_returns_5',
              # 'lower_bb_5',
              # 'upper_bb_5'
              'min_target_5'
              ]],
          file = open("output.txt", "a"))
    df = df.iloc[1:]

    # print('\n\n\n\n\n\n\n',
    #       file=open("output.txt", "a"))
    # print(df[['time', 'open', 'close', 'high', 'low', 'volumeto', 'volumefrom',
    #         'ema_volume_4320',
    #         'ema_close_4320',
    #         'log_ret_4320',
    #         'true_range_4320',
    #         'rel_volume_returns_4320',
    #         'std_close_4320',
    #         'returns_4320',
    #         'std_returns_4320',
    #         'lower_bb_4320',
    #         'upper_bb_4320']],
    #     file=open("output.txt", "a"))

    time.sleep(60.0 - ((time.time() - starttime) % 60.0))






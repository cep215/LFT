import math
import time
import pandas as pd
import requests
import numpy as np
import os


from lft.init_features import create_past_df, log_ret, avg_ret, period_list, target_period_list, alpha_list
from lft.db_def import Aggregate, Kraken


#aici in loc de ~/Desktop/lft/LFT/lft/ trebuie sa pui path-ul unde ai tu directory-ul
os.system("scp ubuntu@ec2-18-224-69-153.us-east-2.compute.amazonaws.com:~/LFT/lft/data.db ~/Desktop/lft/LFT/lft/")

#########################################################
df = create_past_df(Aggregate).iloc[-5000:]
# df = create_past_df(Aggregate).iloc[-10:]
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
            #set avg to null to recalculate features
            df["avg"].loc[df['time'] == row.time] = np.nan


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

    ### Calculate min and max target return
    for period in target_period_list:
        df['min_target_return_' + str(period)] = np.log(df['min_target_' + str(period)] / df['close'])
        df['max_target_return' + str(period)] = np.log(df['max_target_' + str(period)] / df['close'])


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
        for (period, alpha) in zip(period_list, alpha_list):
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
        for (period, alpha) in zip(period_list, alpha_list):
            df['ema_close_' + str(period)].iloc[i] = alpha * df['close'].iloc[i] + (1 - alpha) * df['ema_close_' + str(period)].iloc[i - 1]
            # df['std_close_' + str(period)].iloc[index] = df['close'].rolling(window=period).std()
            df['std_close_' + str(period)].iloc[i] = np.std(df['close'].iloc[(i - period) : i + 1])

            df['lower_bb_' + str(period)].iloc[i] = df['ema_close_' + str(period)].iloc[i] - 2 * df['std_close_' + str(period)].iloc[i]
            df['upper_bb_' + str(period)].iloc[i] = df['ema_close_' + str(period)].iloc[i] + 2 * df['std_close_' + str(period)].iloc[i]


    return df

position1 = 0
position2 = 0

def strategy ( update , unit ):
    timpul = int(update[['time']].iloc[-1])
    position = 0
    global position1
    global position2
    timpul_latent = float(update[['time']].iloc[-3])
    closeul = float(update[['close']].iloc[-3])
    ecl360 = float(update[['ema_close_360']].iloc[-3])
    ecl1440 = float(update[['ema_close_1440']].iloc[-3])
    volumul = float(update[['volumeto']].iloc[-3])
    evl360 = float(update[['ema_volume_360']].iloc[-3])
    evl1440 = float(update[['ema_volume_1440']].iloc[-3])
    if closeul < ecl1440 / 1.02 and volumul < 0.15 * evl1440:
        if position1 + 1 <= 25:
            position += unit
            position1 += 1
    if closeul > 1.02 * ecl1440 and volumul < 0.15 * evl1440:
        if position1 - 1 >= -25:
            position = position - unit
            position1 = position1 - 1
    if closeul < ecl360 / 1.02 and volumul < 0.7 * evl360:
        if position2 + 1 <= 25:
            position += unit
            position2 += 1
    if closeul < 1.02 * ecl360 and volumul < 0.7 * evl360:
        if position2 - 1 >= -25:
            position = position - unit
            position2 = position2 - 1
    if position != 0:
        if position > 0:
            tipul_trade = 'buy'
        else:
            tipul_trade = 'sell'
        volum_trade = abs (position)
        # response = kraken.query_private('AddOrder',
        #                             {'pair': 'XXBTZEUR',
        #                              'type': tipul_trade,
        #                              'ordertype': 'market',
        #                              'volume': volum_trade,
        #                              'leverage': '3'
        #                              })
        # print (datetime.fromtimestamp(timpul))
        # print (response)
    str1 = "At real time " + \
           str (timpul) + " and old time" + \
           str(timpul_latent) + "the strategy tells you to bid " + \
           str(position) + ": \n" "Strategy one position is " + \
           str(position1) +"\nStrategy two position is " + \
           str(position2) + "\nData is: \n" + "close: " + \
           str(closeul) + "\nvolume: " + \
           str(volumul) + "\nema_close_360: " + \
           str(ecl360) + "\nema_close_1440: " + \
           str(ecl1440) + "\nema_volume_360: " + \
           str(evl360) + "\nema_volume_1440: " + \
           str(evl1440) + "\n\n"
    print(str1, file = open("output.txt", "a"))

starttime=time.time()

print (df[['time', 'open', 'close', 'volumeto', 'ema_close_360'
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
              # 'min_target_5',
              # 'min_target_return_5'
              ]],
          file = open("output.txt", "a"))

while True:
    df = update_df_features(df, 'BTC', 'USD', '')
    strategy(df, 0.005)
    print("update df features index ", "\n",
          df[['time', 'open', 'close', 'volumeto', 'ema_close_360'
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
              # 'min_target_5',
              # 'min_target_return_5'
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






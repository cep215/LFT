import math
import time
import pandas as pd
import requests
import numpy as np
import os
import krakenex
from datetime import datetime

import math
import matplotlib
matplotlib.use('TkAgg')
import matplotlib.pyplot as plt

from lft.init_features import create_past_df, period_list, target_period_list, alpha_list
    # , log_ret, avg_ret
from lft.db_def import Aggregate, Kraken

kraken = krakenex.API()
kraken.load_key('/Users/StefanDavid/PycharmProjects/Simulator/venv/kraken.key')

os.system("scp ubuntu@ec2-18-224-69-153.us-east-2.compute.amazonaws.com:~/LFT/lft/data.db ~/Desktop/LFT/lft/")

#########################################################
df = create_past_df(Aggregate).iloc[-5000:]
df = df.reset_index(drop = True)
df = df.convert_objects(convert_numeric = True)
#########################################################


# df.to_csv("Stefan_Test.csv")


def compute_pnl (strategy, starttime, endtime, pretzul, taker_fee):
    pnl = pd.DataFrame (columns = ['TimeStamp', 'USD', 'BTC', 'UnrealisedPnL', 'Traded_Volume'])
    pnl.loc[0] = [starttime - 60, 0.0, 0.0, 0.0, 0.0]
    i = 1

    for index, row in strategy.iterrows ():
        time = int (row ['Time'])
        quantity = float (row ['Trade'])
        price = float (row ['Price'])
        new_row = [0, 0.0, 0.0, 0.0, 0.0]
        new_row [0] = time
        new_row [1] = float(pnl ['USD'].iloc [i - 1]) - quantity * price - abs (quantity * price * taker_fee)
        new_row [2] = float (pnl ['BTC'].iloc [i - 1] + quantity)
        new_row [3] = new_row [1] + new_row [2] * price - abs (new_row [2] * price * taker_fee)
        new_row [4] = abs (quantity)
        pnl.loc [i] = new_row
        i = i + 1
    new_row = [0, 0.0, 0.0, 0.0, 0.0]
    new_row [0] = endtime
    new_row [1] = float(pnl ['USD'].iloc [i - 1])
    new_row [2] = float(pnl ['BTC'].iloc [i - 1])
    new_row [3] = new_row [1] + new_row [2] * pretzul - abs (new_row [2] * pretzul * taker_fee)
    new_row [4] = 0
    pnl.loc [i] = new_row
    return pnl

def plot_pnl (pnl, name):
    startdate = int(pnl.iloc[1][0])
    x = (pnl ['TimeStamp'] [1:] - startdate) / 86400
    y1 = pnl ['UnrealisedPnL'] [1:]
    y2 = pnl ['USD'] [1:]
    y3 = y1 - y2
    plt.figure (figsize = (30, 12))
    plt.title ('PnL Chart')
    plt.subplot (1, 2, 1)
    plt.plot (x, y1, color='black', linewidth=1.5)
    plt.ylabel ('PnL (USD)')
    plt.xlabel ('time(days)')
    plt.legend (['Total PnL'], loc='upper left')
    plt.subplot (1, 2, 2)
    plt.plot (x, y2, color='green', linewidth=1.5)
    plt.plot (x, y3, color='red', linewidth=1.5)
    plt.ylabel ('PnL (USD)')
    plt.xlabel ('time(days)')
    plt.legend (['USD Value', 'BTC Value'], loc='upper left')
    plt.savefig (name+'.png')

def simsummary (pnl, name, name_file):
    sims = pd.DataFrame (columns = ['PnL', 'Max USD', 'Max BTC', 'Returns%', 'Sharpe', 'Traded_Volume', 'No_days'])
    startdate = int (pnl.iloc [1] [0])
    enddate = int (pnl.iloc [-1] [0])
    no_days = float (enddate - startdate) / 86400.0
    sims1 = pnl.iloc [-1] ['UnrealisedPnL']
    sims2 = max ([-n for n in pnl.loc [:] ['USD']])
    sims2 = max (0, sims2)
    sims3 = max ([-n for n in [a_i - b_i for a_i, b_i in zip (pnl.loc [:] ['UnrealisedPnL'], pnl.loc [:] ['USD'])]])
    sims3 = max (0, sims3)
    booksaiz = max (sims2, sims3)
    sims4 = 100 * sims1 / booksaiz
    sims4 = sims4 / no_days
    pnls = []
    for index, row in pnl.iterrows():
        if row ['TimeStamp'] < startdate:
            past = row ['UnrealisedPnL'] + booksaiz
            continue
        actual = row ['UnrealisedPnL'] + booksaiz
        pnls.append (actual / past - 1)
        past = actual
    average = float (sum (pnls)) / float (len (pnls))
    sims5 = average / np.std (pnls)
    sims6 = sum ([a for a in pnl.loc [:] ['Traded_Volume']])
    sims1 = '{0:.2f}'.format (sims1)
    sims2 = '{0:.2f}'.format (sims2)
    sims3 = '{0:.2f}'.format (sims3)
    sims4 = '{0:.3f}'.format (sims4)
    sims5 = '{0:.3f}'.format (sims5)
    sims7 = '{0:.1f}'.format (no_days)
    sims.loc [0] = [sims1, sims2, sims3, sims4, sims5, sims6, sims7]
    print (name, file = open (name_file + ".txt", "a"))
    print (sims, file = open (name_file + ".txt", "a"))


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
    return df



def update_df_features(df, symbol, comparison_symbol, exchange):
    df = update_df(df, symbol, comparison_symbol, exchange)


    index_null = df[df['avg'].isnull()].index

    ### Calculate target_price
    for period in target_period_list:
        min = df[::-1]['low'].rolling(window=period).min().shift()
        df['min_target_' + str(period)] = min
        max = df[::-1]['high'].rolling(window=period).max().shift()
        df['max_target_' + str(period)] = max

    ### Calculate min and max target return
    for period in target_period_list:
        df['min_target_return_' + str(period)]  = np.log(df['min_target_' + str(period)] / df['close'])
        df['max_target_return'  + str(period)]  = np.log(df['max_target_' + str(period)] / df['close'])


    for i in index_null:


        ### Calculate log_ret
        df['avg'].iloc[i] = (df['low'].iloc[i] + df['high'].iloc[i]) / 2
        for period in period_list:
            p1 = math.ceil(period / 24)
            p2 = math.ceil(2 / 3 * period / 24)

            df['avg1_'    + str(period)] = df['avg'].shift(period - p2 + 1).rolling(p1).mean()
            df['avg2_'    + str(period)] = df['avg'].rolling(p2).mean()

            df['log_ret_' + str(period)].iloc[i] = np.log(df['avg2_' + str(period)].iloc[i] / df['avg1_' + str(period)].iloc[i])

            # df['log_ret_old_' + str(period)].iloc[i] = log_ret(df, period, i)



        ### Calculate feature true_range = (max-min)/(max+min) for different periods
        for period in period_list:
            # min = df['low'].rolling(window=period).min()
            min = np.min(df['low'].iloc[(i - period) : i + 1])
            df['min_low_' + str(period)].iloc[i] = min
            # max = df['high'].rolling(window=period).max()
            max = np.max(df['high'].iloc[(i - period) : i + 1])
            df['max_high_' + str(period)].iloc[i]   = max
            df['true_range_' + str(period)].iloc[i] = (max - min) / (max + min)

        ### Calculate feature rel_volume_returns
        for (period, alpha) in zip(period_list, alpha_list):
            # Calculate ema for different spans
            df['ema_volume_' + str(period)].iloc[i] = alpha * df['volumeto'].iloc[i] + (1-alpha) * df['ema_volume_'+ str(period)].iloc[i - 1]

            # Calculate returns on different periods
            # df['returns_' + str(period)].iloc[i] = df['close'].pct_change(periods=period)
            df['returns_' + str(period)].iloc[i] = (df['avg2_' + str(period)].iloc[i] - df['avg1_' + str(period)].iloc[i]) / df['avg1_' + str(period)].iloc[i]
            # df['returns_old_'+str(period)].iloc[i] = avg_ret(df, period, i)

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

starttime=time.time()

###################    GLOBAL BOOK VARIABLES    ###################
# In combo function modify "lista" for new alphas
booksize = 0.25
positions = [3, -0.25, 12, -0.12640067] # number of alphas, positions held assuming alphas have each booksize equal to booksize
weight = [1, 0.4, 0.4, 0.2] # 1, and then the weight of each alpha
###################################################################
# for i193 in range (1, positions[0] + 1, 1):
#     alpha.to_csv (r'alpha'+str(i193)+'.csv')
    # positions.append(0)

# alpha_tong_vol (180, 5, 3, 1.25)
def alpha1 (update):
    global booksize
    global positions
    alpha1df = pd.read_csv("alpha1.csv", usecols=range(1,4))
    position = 0
    test_tong = 57.82911125091462
    scaling = 3 * test_tong / booksize
    max_pos = booksize
    test_tong = test_tong / scaling
    timpul_latent = int(update[['time']].iloc[-3])
    closeul = float(update[['close']].iloc[-3])
    ecl5 = float(update[['ema_close_5']].iloc[-3])
    evl30 = float(update[['ema_volume_30']].iloc[-3])
    evl360 = float(update[['ema_volume_360']].iloc[-3])
    ml180 = float(update[['min_low_180']].iloc[-3])
    mh180 = float(update[['max_high_180']].iloc[-3])
    if evl30 < 2.25 * evl360:
        if timpul_latent % 10800 == 0:
            position = ((ml180 + mh180) / 2 - ecl5) / scaling
            print ("ALPHA1")
            print ("Pozitia Originala")
            print (position)
            if abs(position) >= test_tong * 1.25:
                if position >= 0:
                    position = min (position, max_pos - positions[1])
                if position < 0:
                    position = max (position, - max_pos - positions[1])
                print ("Pozitia Limitata")
                print (position)
                positions[1] += position
                alpha1df.loc[len(alpha1df)] = [timpul_latent, position, closeul]
                alpha1df.to_csv (r'alpha1.csv')
                print (positions)
            else:
                print ("Nu s-a tranzactionat")
                position = 0
    return [position, alpha1df]

# alpha_60_vol (1.02, 12, 2.25)
def alpha2 (update):
    global booksize
    global positions
    alpha2df = pd.read_csv("alpha2.csv", usecols=range(1,4))
    position = 0
    unit = booksize / 12
    timpul_latent = int(update[['time']].iloc[-3])
    closeul = float(update[['close']].iloc[-3])
    ecl60 = float(update[['ema_close_60']].iloc[-3])
    evl30 = float(update[['ema_volume_30']].iloc[-3])
    evl360 = float(update[['ema_volume_360']].iloc[-3])
    if evl30 < 2.25 * evl360:
        if closeul < ecl60 / 1.02:
            if positions[2] + 1 <= 12:
                positions[2] += 1
                position += unit
                alpha2df.loc[len(alpha2df)] = [timpul_latent, position, closeul]
                alpha2df.to_csv(r'alpha2.csv')
                print(positions)
        if closeul > ecl60 * 1.02:
            if positions[2] - 1 >= -12:
                positions[2] = positions[2] - 1
                position = position - unit
                alpha2df.loc[len(alpha2df)] = [timpul_latent, position, closeul]
                alpha2df.to_csv(r'alpha2.csv')
                print(positions)
    return [position, alpha2df]

# alpha_tong (360, 3, 5, 1.0)
def alpha3 (update):
    global booksize
    global positions
    alpha3df = pd.read_csv("alpha3.csv", usecols=range(1,4))
    position = 0
    test_tong = 94.79663551387678
    scaling = 5 * test_tong / booksize
    max_pos = booksize
    test_tong = test_tong / scaling
    timpul_latent = int(update[['time']].iloc[-3])
    closeul = float(update[['close']].iloc[-3])
    ecl3 = float(update[['ema_close_3']].iloc[-3])
    ml360 = float(update[['min_low_360']].iloc[-3])
    mh360 = float(update[['max_high_360']].iloc[-3])

    if timpul_latent % 21600 == 0:
        print ("ALPHA3")
        position = ((ml360 + mh360) / 2 - ecl3) / scaling
        print ("Pozitia Originala")
        print (position)
        if abs(position) >= test_tong * 1.0:
            if position >= 0:
                position = min (position, max_pos - positions[3])
            if position < 0:
                position = max (position, - max_pos - positions[3])
            print ("Pozitia Limitata")
            print (position)
            positions[3] += position
            alpha3df.loc[len(alpha3df)] = [timpul_latent, position, closeul]
            alpha3df.to_csv (r'alpha3.csv')
            print (positions)
        else:
            print ("Nu s-a tranzactionat")
            position = 0
    return [position, alpha3df]

def combo (update):
    global positions
    global weight
    lista = [alpha1(update), alpha2(update), alpha3(update)]
    timpul = int(update[['time']].iloc[-1])
    closeul = float(update[['close']].iloc[-1])
    position = 0

    for i1 in range (0, len(lista), 1):
        position = position + (lista [i1]) [0] * weight [i1 + 1]

    if position != 0:
        if position > 0:
            tipul_trade = 'buy'
        else:
            tipul_trade = 'sell'

        volum_trade = abs(position)
        volum_trade = '{0:.8f}'.format(volum_trade)
        response = kraken.query_private('AddOrder',
                                        {'pair': 'XXBTZEUR',
                                         'type': tipul_trade,
                                         'ordertype': 'market',
                                         'volume': volum_trade,
                                         'leverage': '2'
                                         })
        print (datetime.fromtimestamp(timpul))
        print (response)

    if timpul % 3600 == 0:
        i192 = 0
        for alpha in lista:
            i192 += 1
            if len ( alpha [1] ) > 0:
                pnl = compute_pnl (alpha [1], 1565038800, timpul, closeul, 0.0026)
                plot_pnl (pnl, 'alpha' + str(i192))
                simsummary(pnl, str(datetime.fromtimestamp(timpul)), 'alpha' + str(i192))

while True:
    df = update_df_features (df, 'BTC', 'USD', '')
    ########## STRATEGY ##########
    combo (df)
    ##############################
    df = df.iloc[1:]
    # print(df[['time', 'close', 'avg', 'avg1_5', 'avg2_5', 'returns_5', 'log_ret_360']])
    time.sleep(60.0 - ((time.time() - starttime) % 60.0))
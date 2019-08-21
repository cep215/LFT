import os
import requests
import datetime
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import pandas as pd
import numpy as np
import time
import math
from db_def import Aggregate, Kraken
import itertools

pd.set_option('display.max_rows', 500)
pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 1000)




basedir = os.path.abspath(os.path.dirname(__file__))
engine = create_engine('sqlite:///' + os.path.join(basedir, 'data.db'), echo=True)

# # create a Session
Session = sessionmaker(bind=engine)
session = Session()

period_list = np.array([4320, 1440, 360, 180, 60, 30, 15, 5, 3])
alpha_list = np.array([0.0001604378647097727291148906149522633583756137707677,
                       0.0004812363773337404152339813311276645785804424248118,
                       0.0019235564243712611519494261654683301193732902289814,
                       0.0038434127794247823525750865784463128371033701119830,
                       0.0114859796471038646431324950617078895151506682298763,
                       0.0228400315657540450673018538223454763336870612521282,
                       0.0451583960895834489591009252793566572465208960832665,
                       0.1294494367038758608637299825202539010208745756519969,
                       0.2062994740159002626241471803638458698042533360500734])
target_period_list = np.array([360, 300, 240, 180, 120, 60, 30, 15, 10, 5, 3])


def get_pandas(db):
    query = session.query(db).order_by(db.time)
    df = pd.read_sql(query.statement, session.bind)
    return df


# # Getting the average price for interval (start, start+period)
# def get_avg(df, start, period):
#     avg = df['avg'].iloc[start:(start + period)].mean(axis=0)
#     if (period != 0):
#         return avg
#     else:
#         return 1
#
#
# # Calculate log_ret for a period
# def log_ret(df, period, index):
#
#     p1 = math.ceil(period / 24)
#     p2 = math.ceil(2 / 3 * period / 24)
#     if (index - period > 0):
#         avg_1 = get_avg(df, index - period, p1)
#         avg_2 = get_avg(df, index - p2 + 1, p2)
#     else:
#         avg_1 = 1
#         avg_2 = 1
#
#     if (avg_1 != 0):
#         return math.log(avg_2/avg_1)
#     else:
#         return 0
#
# def avg_ret(df, period, index):
#
#     p1 = math.ceil(period / 24)
#     p2 = math.ceil(2 / 3 * period / 24)
#     if (index - period > 0):
#         avg_1 = get_avg(df, index - period, p1)
#         avg_2 = get_avg(df, index - p2 + 1, p2)
#         # print(avg_1, avg_2)
#     else:
#         avg_1 = 1
#         avg_2 = 1
#     if (avg_1 != 0):
#         return (avg_2 - avg_1)/avg_1
#     else :
#         return 0



def create_past_df(db):

    df = get_pandas(Aggregate)
    #### CAP TO 40,000 ####
    # df = df.iloc[-400:]
    # df = df.reset_index(drop=True)

    ### Calculate min and max target price
    for period in target_period_list:
        min = df[::-1]['low'].rolling(window=period).min().shift()
        df['min_target_' + str(period)] = min
        max = df[::-1]['high'].rolling(window=period).max().shift()
        df['max_target_' + str(period)] = max

    ### Calculate min and max target return
    for period in target_period_list:
        df['min_target_return_' + str(period)] = np.log(df['min_target_' + str(period)] / df['close'])
        df['max_target_return' + str(period)] = np.log(df['max_target_' + str(period)] / df['close'])



    ### Calculate feature true_range = (max-min)/(max+min) for different periods
    for period in period_list:
        min = df['low'].rolling(window=period).min()
        df['min_low_'+str(period)] = min
        max = df['high'].rolling(window=period).max()
        df['max_high_' + str(period)] = max
        df['true_range_'+str(period)] = (max-min)/(max+min)





    ### Calculate feature log(1 + %returns) ###

    df['avg'] = (df['low'] + df['high']) /2

    # Populate df with log_ret_period
    for period in period_list:
        p1 = math.ceil(period / 24)
        p2 = math.ceil(2 / 3 * period / 24)
        df['avg1_' + str(period)] = df['avg'].shift(period - p2 + 1).rolling(p1).mean()
        df['avg2_' + str(period)] = df['avg'].rolling(p2).mean()
        df['log_ret_' + str(period)] = np.log(df['avg2_' + str(period)] / df['avg1_' + str(period)])


    # for period in period_list:
    #     df['log_ret_old_'+str(period)] = 0
    #     logret = np.vectorize(log_ret)
    #     #do not vectorize on df
    #     logret.excluded.add(0)
    #     df['log_ret_old_' + str(period)] = logret(df, period, df.index)



    ### Calculate feature rel_volume_returns
    for (period, alpha) in zip(period_list, alpha_list):
        df['ema_volume_' + str(period)] = df.volumeto.ewm(alpha=alpha, adjust =False).mean()
        df['returns_' + str(period)] = (df['avg2_' + str(period)] - df['avg1_' + str(period)]) / df['avg1_' + str(period)]
        # Calculate ema for different spans
        # for index, row in df.iterrows():
        #     if (index > 0):
        #         df['ema_volume_'+str(period)].iloc[index] = \
        #             alpha * df['volumeto'].iloc[index] + \
        #             (1-alpha) * df['ema_volume_'+ str(period)].iloc[index-1]
        #         # df.volumeto.ewm(halflife=period, adjust=False).mean()
        #     else:
        #         df['ema_volume_' + str(period)].iloc[index] = df['volumeto'].iloc[index]

        # Calculate returns on different periods
        # df['returns_old_' + str(period)] = 0
        # avgret = np.vectorize(avg_ret)
        # avgret.excluded.add(0)
        # df['returns_old_' + str(period)] = avgret(df, period, df.index)

        df['rel_volume_returns_'+str(period)] = df['volumeto']/df['ema_volume_'+str(period)]*df['returns_'+str(period)]




    ### Calculate std_returns
    for period in period_list:
        df['std_returns_'+str(period)] = df['returns_'+str(period)].rolling(window=period).std()



    ### Calculate Bollinger Bands
    for (period, alpha) in zip(period_list, alpha_list):
        df['ema_close_' + str(period)] = df.close.ewm(alpha=alpha, adjust =False).mean()
        # for index, row in df.iterrows():
        #     if (index > 0):
        #         df['ema_close_'+str(period)].iloc[index] = alpha * df['close'].iloc[index] + (1-alpha) * df['ema_close_'+ str(period)].iloc[index-1]
        #         # df.volumeto.ewm(halflife=period, adjust=False).mean()
        #     else:
        #         df['ema_close_' + str(period)].iloc[index] = df['close'].iloc[index]
        df['std_close_'+str(period)] = df['close'].rolling(window=period).std()

        df['lower_bb_'+str(period)] = df['ema_close_'+str(period)] - 2*df['std_close_'+str(period)]
        df['upper_bb_'+str(period)] = df['ema_close_'+str(period)] + 2*df['std_close_'+str(period)]


    # print(df[['lower_bb_60', 'close','upper_bb_60']].tail(100))

    # print(df['high'].iloc[5000], df['low'].iloc[5000], df['avg'].iloc[5000])
    # print(df['high'].iloc[5005], df['low'].iloc[5005], df['avg'].iloc[5005])
    #
    # avg_ret(df, 5, 5005)
    # print(avg_ret(df, 5, 5005))
    # print(df['returns_5'].iloc[5005])

    return df

# df = create_past_df(Aggregate)
# print(df[['time', 'avg', 'avg1_360', 'avg2_360', 'returns_5', 'returns_old_5']])
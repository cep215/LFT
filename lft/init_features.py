import os
import requests
import datetime
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import pandas as pd
import numpy as np
import time
import math
from lft.db_def import Aggregate, Kraken

pd.set_option('display.max_rows', 500)
pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 1000)




basedir = os.path.abspath(os.path.dirname(__file__))
engine = create_engine('sqlite:///' + os.path.join(basedir, 'data.db'), echo=True)

# # create a Session
Session = sessionmaker(bind=engine)
session = Session()

period_list = np.array([4320, 1440, 360, 180, 60, 30, 15, 5, 3])


def get_pandas(db):
    query = session.query(db).order_by(db.time)
    df = pd.read_sql(query.statement, session.bind)
    return df


# Getting the average price for interval (start, start+period)
def get_avg(df, start, period):
    avg = df['avg'].iloc[start:(start + period)].mean(axis=0)
    if (period != 0):
        return avg / period
    else:
        return 1


# Calculate log_ret for a period
def log_ret(period, index, df):

    p1 = math.ceil(period / 24)
    p2 = math.ceil(2 / 3 * period / 24)
    if (index - period > 0):
        avg_1 = get_avg(df, index - period, p1)
        avg_2 = get_avg(df, index - p2, p2)
    else:
        avg_1 = 1
        avg_2 = 1

    if (avg_1 != 0):
        return math.log(avg_2/avg_1)

def create_past_df(db):

    df = get_pandas(Aggregate)
    # print(df.loc[df_aggregate['time'] == '1560857220'])



    ### Calculate ema for different spans
    # alpha = 0.01

    for period in period_list:
        df['ema_'+ str(period)] = df.volumeto.ewm(halflife=period, adjust =
        False).mean()




    ### Calculate feature true_range = (max-min)/(max+min) for different periods
    for period in period_list:
        min = df['low'].rolling(window=period).min()
        max = df['high'].rolling(window=period).max()
        df['true_range_'+str(period)] = (max-min)/(max+min)





    ### Calculate feature log(1 + %returns) ###

    df['avg'] = (df['low'] + df['high']) /2






    # Populate df with log_ret_period
    for period in period_list:
        df['log_ret_'+str(period)] = 0
        v = np.vectorize(log_ret)
        #do not vectorize on df
        v.excluded.add(2)
        df['log_ret_' + str(period)] = v(period, df.index, df)



    # print(log_ret(15, 2003))
    # print(df.at[2003, 'log_ret_15'])




    ### Calculate feature rel_volume_returns
    for period in period_list:
        # Calculate ema for different spans
        df['ema_volume_'+str(period)] = df.volumeto.ewm(halflife=period, adjust=False).mean()

        # Calculate returns on different periods
        df['returns_' + str(period)] = df['close'].pct_change(periods=period)

        df['rel_volume_returns_'+str(period)] = df['volumeto']/df['ema_volume_'+str(period)]*df['returns_'+str(period)]




    ### Calculate std_returns
    for period in period_list:
        df['std_returns_'+str(period)] = df['returns_'+str(period)].rolling(window=period).std()



    ### Calculate Bollinger Bands
    for period in period_list:
        df['ema_close_'+str(period)] = df.close.ewm(span=period, adjust=False).mean()
        df['std_close_'+str(period)] = df['close'].rolling(window=period).std()

        df['lower_bb_'+str(period)] = df['ema_close_'+str(period)] - 2*df['std_close_'+str(period)]
        df['upper_bb_'+str(period)] = df['ema_close_'+str(period)] + 2*df['std_close_'+str(period)]


    # print(df[['lower_bb_60', 'close','upper_bb_60']].tail(100))

    return df


# print(create_past_df(Aggregate))



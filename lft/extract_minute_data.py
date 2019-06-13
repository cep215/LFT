import os
import csv
import requests
import datetime
import pandas as pd

from collections import deque

def tail(filepath):

    try:
        filepath.is_file
        fp = str(filepath)
    except AttributeError:
        fp = filepath

    with open(fp, "rb") as f:
        size = os.stat(fp).st_size
        start_pos = 0 if size - 1 < 0 else size - 1

        if start_pos != 0:
            f.seek(start_pos)
            char = f.read(1)

            if char == b"\n":
                start_pos -= 1
                f.seek(start_pos)

            if start_pos == 0:
                f.seek(start_pos)
            else:
                char = ""

                for pos in range(start_pos, -1, -1):
                    f.seek(pos)

                    char = f.read(1)

                    if char == b"\n":
                        break

        return f.readline()

def minute_price_historical(symbol, comparison_symbol, exchange):
    url = 'https://min-api.cryptocompare.com/data/histominute?fsym={}&tsym={}'\
            .format(symbol.upper(), comparison_symbol.upper())
    if exchange:
        url += '&e={}'.format(exchange)
    page = requests.get(url)
    data = page.json()['Data']
    df = pd.DataFrame(data)
    df['timestamp'] = [datetime.datetime.fromtimestamp(d) for d in df.time]
    return df

df_CCCAGG = minute_price_historical('BTC', 'USD', '')
df_CCCAGG = df_CCCAGG.rename(columns={
    'close': 'close_CCCAGG',
    'high': 'high_CCCAGG',
    'low': 'low_CCCAGG',
    'open': 'open_CCCAGG',
    'volumefrom': 'volumefrom_CCCAGG',
    'volumeto': 'volumeto_CCCAGG'})
#df_kraken = minute_price_historical('BTC', 'USD', 'kraken')
#df_kraken = df_kraken.rename(columns={
#    'close': 'close_kraken',
#    'high': 'high_kraken',
#    'low': 'low_kraken',
#    'open': 'open_kraken',
#    'volumefrom': 'volumefrom_kraken',
#    'volumeto': 'volumeto_kraken'})

#result = pd.merge(df_CCCAGG, df_kraken, on=['timestamp', 'time'])
#result = result.set_index('timestamp')
#csv_result = result.to_csv(index=True)

def get_last_row(csv_filename):
    with open("../data.csv", 'rb') as f:
        return deque(csv.reader(f), 1)[0]

lastline = ', '.join(get_last_row('../data.csv'))
values = lastline.split("\t")
print (tail("../data.csv"))

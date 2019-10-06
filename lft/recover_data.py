from lft.recover_db_def import Aggregate as Aggregate_recover
from lft.recover_db_def import Kraken as Kraken_recover

from lft.recover_db_def import session as session_recover

from lft.db_def import Aggregate, Kraken, session

import pandas as pd

def get_pandas(db, session):
    query = session.query(db).filter_by(from_currency='BTC').filter_by(to_currency='USD').order_by(db.time)
    df = pd.read_sql(query.statement, session.bind)
    df = df.reset_index(drop=True)
    return df

def insert_df_aggregate(df, from_currency, to_currency):
    for index, row in df.iterrows():
        # access data using column names]
        item = session.query(Aggregate).filter_by(from_currency=from_currency).filter_by(to_currency=to_currency).filter_by(time=row['time']).first()
        if (item is None):
            agg = Aggregate(row['time'], row['close'], row['high'], row['low'],
                            row['open'], row['volumefrom'], row['volumeto'], from_currency=from_currency, to_currency=to_currency)
            session.add(agg)

    # commit the record to the database
    session.commit()


def insert_df_kraken(df, from_currency, to_currency):


    # Create objects
    for index, row in df.iterrows():
        # access data using column names
        item = session.query(Kraken).filter_by(from_currency=from_currency).filter_by(to_currency=to_currency).filter_by(time=row['time']).first()
        if (item is None):
            krk = Kraken(row['time'], row['close'], row['high'], row['low'],
                         row['open'], row['volumefrom'], row['volumeto'], from_currency=from_currency, to_currency=to_currency)
            session.add(krk)

    # commit the record to the database
    session.commit()

df_aggregate_recover = get_pandas(Aggregate_recover, session_recover)
df_kraken_recover = get_pandas(Kraken_recover, session_recover)


df_aggregate = get_pandas(Aggregate, session)
df_aggregate_recover = pd.merge(df_aggregate_recover, df_aggregate, on=['time', 'close', 'high', 'low', 'open', 'volumeto', 'volumefrom', 'from_currency', 'to_currency'], how='outer', indicator=True)\
    .query("_merge != 'both'")\
    .drop('_merge', axis=1).reset_index(drop=True)


df_kraken = get_pandas(Kraken, session)
df_kraken_recover = pd.merge(df_kraken_recover, df_kraken, on=['time', 'close', 'high', 'low', 'open', 'volumeto', 'volumefrom', 'from_currency', 'to_currency'], how='outer', indicator=True)\
    .query("_merge != 'both'")\
    .drop('_merge', axis=1).reset_index(drop=True)

insert_df_aggregate(df_aggregate_recover, 'BTC', 'USD')
insert_df_kraken(df_kraken_recover, 'BTC', 'USD')
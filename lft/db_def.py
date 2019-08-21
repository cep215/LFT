from sqlalchemy import *
import os
from sqlalchemy import create_engine, ForeignKey
from sqlalchemy import Column, Date, Integer, String, Float, TIMESTAMP
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship, backref

basedir = os.path.abspath(os.path.dirname(__file__))
engine = create_engine('sqlite:///' + os.path.join(basedir, 'data.db'), echo=True)
Base = declarative_base()


########################################################################
class Aggregate(Base):
    """"""
    __tablename__ = "aggregate"

    id = Column(Integer, primary_key=True)
    time = Column(String)
    close = Column(Float)
    high = Column(Float)
    low = Column(Float)
    open = Column(Float)
    volumefrom = Column(Float)
    volumeto = Column(Float)
    from_currency = Column(String, default='BTC')
    to_currency = Column(String, default='USD')
    exchange = Column(String, default='CCCAG')

    # ----------------------------------------------------------------------
    def __init__(self, time, close, high, low, open, volumefrom, volumeto, from_currency = 'BTC', to_currency = 'USD', exchange = 'CCCAG'):
        """"""
        self.time = time
        self.close = close
        self.high = high
        self.low = low
        self.open = open
        self.volumefrom = volumefrom
        self.volumeto = volumeto
        self.from_currency = from_currency
        self.to_currency = to_currency
        self.exchange = exchange

class Kraken(Base):
    """"""
    __tablename__ = "kraken"

    id = Column(Integer, primary_key=True)
    time = Column(String)
    close = Column(Float)
    high = Column(Float)
    low = Column(Float)
    open = Column(Float)
    volumefrom = Column(Float)
    volumeto = Column(Float)
    from_currency = Column(String, default='BTC')
    to_currency = Column(String, default='USD')
    exchange = Column(String, default='kraken')

    # ----------------------------------------------------------------------
    def __init__(self, time, close, high, low, open, volumefrom, volumeto, from_currency = 'BTC', to_currency = 'USD', exchange = 'kraken'):
        """"""
        self.time = time
        self.close = close
        self.high = high
        self.low = low
        self.open = open
        self.volumefrom = volumefrom
        self.volumeto = volumeto
        self.from_currency = from_currency
        self.to_currency = to_currency
        self.exchange = exchange


# class Bitmex(Base):
#     """"""
#     __tablename__ = "bitmex"
#
#     id = Column(Integer, primary_key=True)
#     time = Column(String, unique=True)
#     close = Column(Float)
#     high = Column(Float)
#     low = Column(Float)
#     open = Column(Float)
#     volumefrom = Column(Float)
#     volumeto = Column(Float)
#     from_currency = Column(String, default='BTC')
#     to_currency = Column(String, default='USD')
#     exchange = Column(String, default='bitmex')
#
#     # ----------------------------------------------------------------------
#     def __init__(self, time, close, high, low, open, volumefrom, volumeto, from_currency = 'BTC', to_currency = 'USD', exchange = 'bitmex'):
#         """"""
#         self.time = time
#         self.close = close
#         self.high = high
#         self.low = low
#         self.open = open
#         self.volumefrom = volumefrom
#         self.volumeto = volumeto
#         self.from_currency = from_currency
#         self.to_currency = to_currency
#         self.exchange = exchange


# create tables
Base.metadata.create_all(engine)

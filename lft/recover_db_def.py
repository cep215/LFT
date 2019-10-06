import os
from sqlalchemy import create_engine
from sqlalchemy import Column, Date, Integer, String, Float, TIMESTAMP
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

basedir = os.path.abspath(os.path.dirname(__file__))
engine  = create_engine('sqlite:///' + os.path.join(basedir, 'data_amazon.db'), echo = True)
Base    = declarative_base()


# create a SQLAlchemy ORM Session
Session = sessionmaker(bind = engine)
session = Session()


########################################################################
class Aggregate(Base):
    """"""
    __tablename__ = "aggregate"

    id            = Column(Integer, primary_key = True)
    time          = Column(String)
    close         = Column(Float)
    high          = Column(Float)
    low           = Column(Float)
    open          = Column(Float)
    volumefrom    = Column(Float)
    volumeto      = Column(Float)
    from_currency = Column(String,  default = 'BTC')
    to_currency   = Column(String,  default = 'USD')
    exchange      = Column(String,  default = 'CCCAG')


    # ----------------------------------------------------------------------
    def __init__(self, time, close, high, low, open, volumefrom, volumeto, from_currency = 'BTC', to_currency = 'USD', exchange = 'CCCAG'):
        """"""
        self.time          = time
        self.close         = close
        self.high          = high
        self.low           = low
        self.open          = open
        self.volumefrom    = volumefrom
        self.volumeto      = volumeto
        self.from_currency = from_currency
        self.to_currency   = to_currency
        self.exchange      = exchange

class Kraken(Base):
    """"""
    __tablename__ = "kraken"


    id            = Column(Integer, primary_key = True)
    time          = Column(String)
    close         = Column(Float)
    high          = Column(Float)
    low           = Column(Float)
    open          = Column(Float)
    volumefrom    = Column(Float)
    volumeto      = Column(Float)
    from_currency = Column(String,  default = 'BTC')
    to_currency   = Column(String,  default = 'USD')
    exchange      = Column(String,  default = 'kraken')


    # ----------------------------------------------------------------------
    def __init__(self, time, close, high, low, open, volumefrom, volumeto, from_currency = 'BTC', to_currency = 'USD', exchange = 'kraken'):
        """"""
        self.time          = time
        self.close         = close
        self.high          = high
        self.low           = low
        self.open          = open
        self.volumefrom    = volumefrom
        self.volumeto      = volumeto
        self.from_currency = from_currency
        self.to_currency   = to_currency
        self.exchange      = exchange


# create tables
Base.metadata.create_all(engine)

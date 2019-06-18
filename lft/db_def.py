from sqlalchemy import *
from sqlalchemy import create_engine, ForeignKey
from sqlalchemy import Column, Date, Integer, String, Float, TIMESTAMP
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship, backref

engine = create_engine('sqlite:///data.db', echo=True)
Base = declarative_base()


########################################################################
class Aggregate(Base):
    """"""
    __tablename__ = "aggregate"

    id = Column(Integer, primary_key=True)
    time = Column(String, unique=True)
    close = Column(Float)
    high = Column(Float)
    low = Column(Float)
    open = Column(Float)
    volumefrom = Column(Float)
    volumeto = Column(Float)

    # ----------------------------------------------------------------------
    def __init__(self, time, close, high, low, open, volumefrom, volumeto):
        """"""
        self.time = time
        self.close = close
        self.high = high
        self.low = low
        self.open = open
        self.volumefrom = volumefrom
        self.volumeto = volumeto

class Kraken(Base):
    """"""
    __tablename__ = "kraken"

    id = Column(Integer, primary_key=True)
    time = Column(String, unique=True)
    close = Column(Float)
    high = Column(Float)
    low = Column(Float)
    open = Column(Float)
    volumefrom = Column(Float)
    volumeto = Column(Float)

    # ----------------------------------------------------------------------
    def __init__(self, time, close, high, low, open, volumefrom, volumeto):
        """"""
        self.time = time
        self.close = close
        self.high = high
        self.low = low
        self.open = open
        self.volumefrom = volumefrom
        self.volumeto = volumeto


# create tables
Base.metadata.create_all(engine)
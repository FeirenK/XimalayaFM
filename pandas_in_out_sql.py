#!/usr/bin/env python

import os
import numpy as np
import pandas as pd
from pandas import DataFrame
from sqlalchemy.engine import create_engine
import datetime

auth = ["user","passwd","xxxxx.rds.amazonaws.com","3306","hft"]
mysqlengine = create_engine("mysql+mysqldb://{0}:{1}@{2}:{3}/{4}?charset=utf8".format(auth[0],auth[1],auth[2],auth[3],auth[4]))
df = pd.read_sql_table('hft_trade',mysqlengine,index_col='created_at')
trade_pair_all = ["BCHBTC", "EOSBTC", "ETHBTC", "LTCBTC", "TRXBTC"]
types = ['1min','5min','15min','30min','H','D','W']
#trade_pair_all = ["BCHBTC"]
#types = ['1min']

def pandas_ohlc_k(trading_pair,rule):
    #allfiles = glob.glob(path + trading_pair + "/0000*")
    #print allfiles
    names = ['time','price','quantity']
    columns = ['time','open','high','low','close','quantity']

    #import datetime
    #current = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    #current = pd.to_datetime(current) + pd.Timedelta(minutes=3)
    #loadfile.loc[current] = [np.nan,0]
    # loadfile = loadfile.groupby('time').agg({'open': 'max','quantity': 'sum'})

    ohlc = trading['price'].resample(rule,label='left',closed='right',convention='start').ohlc()
    # ohlc = loadfile['price'].resample(rule,label='right',closed='right',convention='end').fillna(method='ffill')
    quantity = trading['quantity'].resample(rule,label='left',closed='right',convention='start').sum()
    # rng = pd.date_range(start=oldtime,freq='D',end=current)
    newpd = pd.concat([ohlc,quantity],axis=1,ignore_index=False).reset_index()
    newpd.index += 1 # (Index is an object, and default index starts from 0:) change to 1
    #if NAN change to last value
    # newpd.fillna(method='ffill', inplace=True)
    newpd.close.fillna(method='pad',inplace=True)
    newpd.open.fillna(newpd.close,inplace=True)
    newpd.high.fillna(newpd.close,inplace=True)
    newpd.low.fillna(newpd.close,inplace=True)
    if rule == 'D':
       rule = 'B'
    table = trading_pair + "_" + rule
    newpd.to_sql(table, mysqlengine, schema='hft_trade', if_exists='replace', index=True, index_label='id')



for trade_pair in trade_pair_all:
    trading = df[df['trading_pair'] == trade_pair]
    trading = trading[['price','quantity']]
    ##append data to current time
    current = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    current = pd.to_datetime(current)
    trading.loc[current] = [np.nan,0]
    ###
    for rule in types:
        pandas_ohlc_k(trade_pair,rule)

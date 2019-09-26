#!/usr/bin/env python
#encoding:utf8
'''
python pandas load hive data to mysql
'''
import os
import numpy as np
import pandas as pd
from sqlalchemy.engine import create_engine

#method 1
auth = ["devtool","devtool123","172.31.40.192","3306","hft_trade"]
mysqlengine = create_engine("mysql+mysqldb://{0}:{1}@{2}:{3}/{4}?charset=utf8".format(auth[0],auth[1],auth[2],auth[3],auth[4]))
hiveengine = create_engine('hive://localhost:10000/default')

#if use read_sql_query
sql = 'select created_at,price,quantity,trading_pair from hft.hft_trade'
hivedata = pd.read_sql_query(sql,hiveengine,parse_dates='created_at') #index_col='created_at',
#else use read_sql_table
# hivedata = pd.read_sql_table('hft_trade',hiveengine)
# hivedata = hivedata[['trading_pair','created_at','price','quantity']]

trade_type_all = hivedata['trading_pair'].drop_duplicates().tolist() #去除,转换为list
types = ['1min','5min','15min','30min','H','B','W']

for trade_type in trade_type_all:
    for rule in types:

        trade_type_data = hivedata[hivedata['trading_pair'] == str(trade_type)].set_index('created_at')
        ohlc = trade_type_data['price'].resample(rule,label='right',closed='right',convention='end').ohlc()
        quantity = trade_type_data['quantity'].resample(rule,label='right',closed='right',convention='end').sum()
        newpd = pd.concat([ohlc,quantity],axis=1,ignore_index=False).reset_index()
        newpd.index += 1 # (Index is an object, and default index starts from 0:) change to 1
        newpd.to_sql(trade_type+"_"+rule,mysqlengine,if_exists='replace', index=True)

#method 2
def hive_get_trading_pair(hivehost,sql):

    # from TCLIService.ttypes import TOperationState
    conn = hive.Connection(host=hivehost, port=10000, username="hadoop",database='hft')
    try:
        cursor = conn.cursor()

    except Exception as err:
        print str(err)
    # else:
    cursor.execute(sql,async=True)
    status = cursor.poll().operationState
    ret = cursor.fetchall()
    return ret


sql = 'select trading_pair from hft.hft_trade group by trading_pair'

def pandas_data(hivehost,trade_type,rule)

    conn = hive.Connection(host=hivehost, port=10000, username="hadoop",database='hft')
    hivesql = 'select created_at,price,quantity from hft_trade where trading_pair = "%s"' % (trade_type)
    # if use parse_dates set datetime col
    hivedata = pd.read_sql(hivesql, conn, index_col='created_at',parse_dates='created_at')
    # else not use parse_dates set datetime col
    # hivedata.index = pd.to_datetime(hivedata.index)
    # hivedata = hivedata.loc[:, ['price', 'quantity']]
    ohlc = hivedata['price'].resample(rule,label='right',closed='right',convention='end').ohlc()
    quantity = hivedata['quantity'].resample(rule,label='right',closed='right',convention='end').sum()
    newpd = pd.concat([ohlc,quantity],axis=1,ignore_index=False).reset_index()
    newpd.index += 1 # (Index is an object, and default index starts from 0:) change to 1
    #if NAN change to last value
    # newpd.fillna(method='ffill', inplace=True)
    return newpd

    # newpd.to_sql(trade_type+"_"+rule,conn)

def mysql_create_table(host,user,pass,db,data,table)


    #db = MySQLdb.connect(host="192.168.99.197", user="maxwell", passwd="viewfin_2018", db="hft_trade")
    db = MySQLdb.connect(host=host,user=user,passwd=pass,db=db,table)
    cursor = db.cursor()

    create_table = "drop table if exists {table}_{rule};create table IF NOT EXISTS {table}_{rule} (id INT AUTO_INCREMENT PRIMARY KEY, created_at timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP, open bigint unsigned NOT NULL, high bigint unsigned NOT NULL, low bigint unsigned NOT NULL, close bigint unsigned NOT NULL, quantity bigint unsigned NOT NULL) ENGINE = InnoDB;".format(table=table,rule=rule)
    try:
        cursor.execute(create_table)
    except:
        db.rollback()




if __name__ == '__main__':
    hivehost = '127.0.0.1'
    types = ['1min','5min','15min','30min','H','B','W']
    sql = 'select trading_pair from hft.hft_trade group by trading_pair'
    trade_type_all = hive_get_trading_pair(hivehost,sql)
    for i in trade_type_all:
        trade_type = i[0]
        pandas_data(hivehost,trade_type)
        mysql_create_table()


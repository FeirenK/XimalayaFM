#!/usr/bin/env python
#encoding:utf8
'''
python pandas load hive data
'''
import os
import numpy as np
import pandas as pd
from pandas import DataFrame
import MySQLdb

path = "/home/hadoop/hft_trade/"
groupby = "/home/hadoop/groupby/"
files = os.listdir(path)


# test[0:16] + ":00"
def _ceiling_price(g):
    return g.idxmin() < g.idxmax() and np.max(g) or (np.max(g))


def mysql_load(tablefile,table):  #192.168.99.197 -umaxwell -pviewfin_2018
    db = MySQLdb.connect(host="192.168.99.197", # your host, usually localhost
                     user="maxwell", # your username
                      passwd="passwd", # your password
                      db="hft_trade") # name of the data base


    cursor = db.cursor()
    # create table
    create_table = "create table IF NOT EXISTS %s (id INT AUTO_INCREMENT PRIMARY KEY, created_at timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP, open_price bigint unsigned NOT NULL, high_price bigint unsigned NOT NULL, low_price bigint unsigned NOT NULL, close_price bigint unsigned NOT NULL, quantity bigint unsigned NOT NULL) ENGINE = InnoDB;"  % (table)
    try:
        cursor.execute(create_table)
    except:
        db.rollback()

    load = "load data local infile %s replace into table %s character set utf8 fields terminated by ',' enclosed by '\"' lines terminated by '\n'" % (tablefile,table)
    try:
        cursor.execute(load)
        db.commit() # mysql can insert ok!
    except:
        db.rollback()
    finally:
        db.close()

# def pandas_groupby():


#     # agg = {'open_price': 'first', 'high_price': _ceiling_price, 'low_price': 'min', 'close_price': 'last', 'quantity': 'sum'}

#     # time_type = {'one_min': lambda x:x[0:16] ,
#     #             'half_hour': lambda x:x[0:14]+'00',
#     #             'one_hour': lambda x:x[0:14]+'00',
#     #             'one_day': lambda x:x[0:10]+' 00:00'}
#     # columns = ['time','open_price','high_price','low_price','close_price','quantity']

#     # for file in files:
#     #     trade_type = path + file
#     #     loadfile = pd.read_csv(trade_type,names=['time','open_price','high_price','low_price','close_price','quantity'],header=None, sep='\t', parse_dates=True,infer_datetime_format=True, memory_map=True)
#     #     for i in time_type:
#     #         timefile, maplambda = i, time_type.get(i)
#     #         groupby = loadfile.groupby(loadfile['time'].map(maplambda)).agg(agg).reset_index()
#     #         outfile = home + file + i
#     #         groupby.to_csv(outfile,index=True,header=False,columns=columns)



#     # per_1min = loadfile.groupby('time').agg({'quantity': 'sum','low_price': 'min', 'high_price': _ceiling_price, 'open_price': 'first','close_price': 'last'})

#     one_min = loadfile.groupby(loadfile['time'].map(lambda x:x[0:10])).agg(agg).reset_index()
#     outfile = hft_trade_1min + file + "_1min"
#     per_1min.to_csv(outfile,index=True,header=False,columns=columns) #index=False,header=False) 即是把index和columns都弃掉，header表示columns


def hive_get_trade_pair(hivehost,sql):
    from pyhive import hive
    # from TCLIService.ttypes import TOperationState
    conn = hive.Connection(host=hivehost, port=10000, username="hadoop",database='hft')
    try:
        cursor = conn.cursor()

    except Exception as err:
        print str(err)
    # else:
    cursor.execute(sql,async=True)
    # status = cursor.poll().operationState
    ret = cursor.fetchall()
    return ret


def hive_sql_file(path_data,trade_pair_all):

    for i in trade_pair_all:
        trade_pair = i[0]
        sql = "insert overwrite local directory '%s' ROW FORMAT DELIMITED  FIELDS TERMINATED BY ',' select created_at,price,quantity from hft.hft_trade where trading_pair = '%s';" % ((path_data+trade_pair),trade_pair)
        with open(hive_sql_file,'w') as writer: # w / a append
            writer.write( sql +'\n')

def pandas_ohlc_k(trading_pair,rule):
    allfiles = glob.glob(path + trading_pair + "/0000*")
    print allfiles
    names = ['time','price','quantity']
    columns1 = ['open','high','low','close','quantity']
    # usecols = [0,1,2]
    list_of_dfs = [pd.read_csv(filename,names=names,header=None,index_col='time',parse_dates=True,infer_datetime_format=True,memory_map=True) for filename in allfiles]

    loadfile = pd.read_csv(path+trading_pair,names=names,header=None,index_col='time',sep=',',parse_dates=True,infer_datetime_format=True,memory_map=True,usecols=usecols)

    loadfile = pd.concat(list_of_dfs,ignore_index=False)
    import datetime
    current = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    current = pd.to_datetime(current) + pd.Timedelta(minutes=3)
    loadfile.loc[current] = [np.nan,0]
    # loadfile = loadfile.groupby('time').agg({'open': 'max','quantity': 'sum'})

    ohlc = loadfile['price'].resample(rule,label='right',closed='right',convention='end').ohlc()
    # ohlc = loadfile['price'].resample(rule,label='right',closed='right',convention='end').fillna(method='ffill')
    quantity = loadfile['quantity'].resample(rule,label='right',closed='right',convention='end').sum()
    # rng = pd.date_range(start=oldtime,freq='D',end=current)
    newpd = pd.concat([ohlc,quantity],axis=1,ignore_index=False).reset_index()
    newpd.index += 1 # (Index is an object, and default index starts from 0:) change to 1
    #if NAN change to last value
    # newpd.fillna(method='ffill', inplace=True)
    newpd.close.fillna(method='pad',inplace=True)
    newpd.open.fillna(newpd.close,inplace=True)
    newpd.high.fillna(newpd.close,inplace=True)
    newpd.low.fillna(newpd.close,inplace=True)
    # newpd.drop(newpd.index[-1],inplace=True)
    #elif delete NAN rows
    # newpd = pd.concat([ohlc,quantity],axis=1,ignore_index=False).dropna(axis=0).reset_index()
    #outfile cols order
    outfile = pathmysql + file + "_" + rule
    columns = ['time','open','high','low','close','quantity']
    newpd.to_csv(outfile,index=True,header=False,columns=columns)

# time = [i[0] for i in trade_pair_data]
# test = pd.DataFrame(trade_pair_data,columns=['time','price','quantity'],index=time)
# test.drop('time',inplace=True,axis=1)
#     test.index.names = ['time']



if __name__ == '__main__':
    types = ['1min','5min','15min','30min','H','B','W']
    #H = 1 hour,B = 1 business day ,W = 1 week
    sql = 'select trading_pair from hft.hft_trade group by trading_pair'
    trade_pair_all = hive_get_trade_pair('127.0.0.1',sql)
    path_data = "/home/hadoop/hft_trade/"
    hive_sql_file = "/home/hadoop/trade_pair_hive.sql"
    import subprocess
    p = subprocess.Popen('hive -f %s' % hive_sql_file, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    p.wait()  #if 1
    p.communicate()

    time.sleep(120s)
    # if path_data
    # for i in trade_pair_all:
    #     trade_pair = i[0]
    #     print trade_pair
    #     hive_trading_pair_data('127.0.0.1',path_data,trade_pair)
    for table in trade_pair_all:
        for rule in types:

            pandas_ohlc_k(table,rule)

["BCHBTC", "EOSBTC", "ETHBTC", "LTCBTC", "TRXBTC"]

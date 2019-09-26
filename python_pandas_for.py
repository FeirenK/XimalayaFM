#!/usr/bin/env python
#encoding:utf8
'''
循环取文件一行与整个文件计算(根据kk文件格式)对比
'''

import pandas as pd
import numpy as np
import sys, getopt
import time,datetime

def main(argv):
    inputfile = 'cata.txt'
    outputfile = 'test.log'
    try:
       opts, args = getopt.getopt(argv,"hi:o:",["ifile=","ofile="])
    except getopt.GetoptError:
        print(sys.argv[0] + ' -i <inputfile> -o <outputfile>')
        sys.exit(2)
    for opt, arg in opts:
        if opt == '-h':
            print(sys.argv[0] + ' -i <inputfile> -o <outputfile>')
            sys.exit()
        elif opt in ("-i", "--ifile"):
            inputfile = arg
        elif opt in ("-o", "--ofile"):
            outputfile = arg
    print('输入的文件为：', inputfile)
    print('输出的文件为：', outputfile)
    return(inputfile,outputfile)


def pandas_for(inputfile,outputfile):

    cata_index = ['t','j','w','x','y','d','m']
    kk_index = ['line','juli','time']

    aftershock = pd.DataFrame(columns=cata_index)
    cata = pd.read_csv(inputfile,names=cata_index,header=None,delim_whitespace=True)
    # cata = cata.eval('xyd = x + y + d')  #新建一列xyd
    # cata = cata.drop(['xyd'],axis=1)     #删除一列xyd
    ###大于$7 > 8 的行数单独保存
    mgreater2_8 = cata[(8 < cata['m']) | (2 > cata['m'])]
    mgreater2_8.to_csv('python_mgreater8.txt',sep=' ',mode='w',index=False,header=False)

    # ###过滤大于$7 > 8 的行数
    tem = cata[(8 >= cata['m']) | (2 <= cata['m'])]
    cata['j'] = cata['j'].round(decimals=3)
    cata['w'] = cata['w'].round(decimals=3)
    cata['t'] = cata['t'].round(decimals=9)
    cata['x'] = cata['x'].round(decimals=7)
    cata['y'] = cata['y'].round(decimals=7)
    kk = pd.read_csv('K-K.txt',names=kk_index,header=None, index_col='line',delim_whitespace=True)

    for i in range(len(tem)):

        m = tem.iloc[i]['m']
        if m >= 2 and m < 2.5:
            line = 9
            R = 30
            Rt = 0.0164383561
        # only level_9,mgreater2_8
        elif m >= 2.5 and m < 3.5:
            line = 8
            R = 30
            Rt = 0.0328767123
            # only level_9,level_8
        elif m >= 3.5 and m < 4:
            line = 7
            R = 40
            Rt = 0.0630136986

        elif m >= 4 and m < 4.5:
            line = 6
            R = 40
            Rt = 0.1260273972
        elif m >= 4.5 and m < 5:
            line = 5
            R = 40
            Rt = 0.2520547945
        elif m >= 5 and m < 5.5:
            line = 4
            R = 50
            Rt = 0.5013698630
        elif m >= 5.5 and m < 6.5:
            line = 3
            R = 50
            Rt = 1
        elif m >= 6.5 and m < 7:
            line = 2
            R = 100
            Rt = 1.501369863
        elif m >= 7 and m < 7.5:
            line = 1
            R = 100
            Rt = 2
        elif m >= 7.5 and m < 8:
            line = 0
            R = 150
            Rt = 2.501369863
        else:
    #        print(str(m) + 'not in 2~8')
            continue

        # R = kk['juli'].values[line]
        # Rt = kk['time'].values[line]
        x = tem.iloc[i]['x']
        y = tem.iloc[i]['y']
        d = tem.iloc[i]['d']
        xydr1 = (x+y+d)-3*R
        xydr2 = (x+y+d)+3*R
        t = tem.iloc[i]['t']
        t1 = t + Rt
        tem_1 = tem[(tem['t'] <= t1) & (tem['t'] > t)]
        tem_1 = tem_1[(xydr1 <= tem_1['x']+tem_1['y']+tem_1['d']) & (tem_1['x']+tem_1['y']+tem_1['d'] <= xydr2) & (tem_1['m'] < m) & (np.sqrt(((tem_1['x'] - x) * (tem_1['x'] - x) + (tem_1['y'] - y) * (tem_1['y'] - y) + (tem_1['d'] - d) * (tem_1['d'] - d))) < R)]

        #filename = "tem_1_py_" + str(line) + ".txt"
        #tem_1.to_csv(filename,sep=' ',mode='a',index=False,header=False)
        aftershock = aftershock.append(tem_1)
    aftershock = aftershock.drop_duplicates()
    aftershock.sort_values('t',inplace=True)
    ###计算cata 与aftershock 差集 得到catalog_delete
    cata = cata.append(aftershock)
    cata = cata.append(aftershock)
    catalog_delete = cata.drop_duplicates(keep=False)
    catalog_delete.sort_values('t',inplace=True)

    ### 格式化数据 x,y,t保留末尾0
    aftershock['t'] = aftershock['t'].map(lambda x: '%2.9f' % x)
    aftershock['x'] = aftershock['x'].map(lambda x: '%2.7f' % x)
    aftershock['y'] = aftershock['y'].map(lambda x: '%2.7f' % x)
    aftershock_file = 'aftershock' + outputfile
    aftershock.to_csv(aftershock_file,sep=' ',mode='w',index=False,header=False)

    ### 格式化数据 x,y,t保留末尾0
    catalog_delete['t'] = catalog_delete['t'].map(lambda x: '%2.9f' % x)
    catalog_delete['x'] = catalog_delete['x'].map(lambda x: '%2.7f' % x)
    catalog_delete['y'] = catalog_delete['y'].map(lambda x: '%2.7f' % x)
    catalog_delete_file = 'catalog_delete' + outputfile
    catalog_delete.to_csv(catalog_delete_file,sep=' ',mode='w',index=False,header=False)


if __name__ == "__main__":
    try:
        argv = main(sys.argv[1:])
    except getopt.GetoptError:
        sys.exit(2)
    inputfile = argv[0]
    outputfile = argv[1]
    pandas_for(inputfile,outputfile)

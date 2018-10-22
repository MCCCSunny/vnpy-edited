# -*- coding: utf-8 -*-
"""
Created on Thu Nov 23 21:43:36 2017

@author: Goofy
"""

# 画出买入卖出点
#------------------------------------------------------------------

import pymongo
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import datetime

def findIndex(tradedf,df,order):
    '''
    tradedf为交易数据，df为行情数据，order为要查找的发单情况
    当数据中没有交易时间的数据时，向前推一分钟，看在不在直到查找到为止
    '''
    if order == 'buy':
        Trade = tradedf[(tradedf.direction == u'多') & (tradedf.offset==u'开仓')]
    elif order == 'sell':
        Trade = tradedf[(tradedf.direction == u'空') & ((tradedf.offset == u'平仓')|(tradedf.offset == u'平昨')|(tradedf.offset == u'平今'))]
    elif order == 'short':
        Trade = tradedf[(tradedf.direction == u'空') & (tradedf.offset==u'开仓')]
    elif order == 'cover':
        Trade = tradedf[(tradedf.direction == u'多') & ((tradedf.offset == u'平仓')|(tradedf.offset == u'平昨')|(tradedf.offset == u'平今'))]
    else:
        print "order有误，请检查"
        return
    index = []
    for i in range(len(Trade)):
        temp = Trade.iloc[i]['time']
        temp = datetime.datetime.strptime(str(temp),'%Y-%m-%d %H:%M:%S').replace(second=0)
        while temp not in df.datetime.tolist():
            temp += datetime.timedelta(minutes=-1)
        if temp in df.datetime.tolist():
            ind = df.datetime.tolist().index(temp)
        index.append(ind)
    return index

if __name__ == '__main__':
    #历史数据库
    host = 'localhost'    
    port = 27017
    dbClient = pymongo.MongoClient(host, port)
    dbName = 'VnTrader_1MIN_ALL_0104'
    #交易日志在本地数据库
    host_trade = 'localhost'
    port_trade = 27017
    dbClient_trade = pymongo.MongoClient(host_trade, port_trade)
    tradeDbName = 'VnTrader_Trade_Db1'
    #collectionNames = ['BullMACDStrategy_rb1805']
    collectionNames = dbClient_trade[tradeDbName].collection_names()
    #需显示的交易日列表
    TradingDay = ['20180105']
    for collName in collectionNames:
        print collName
        symbol = collName.split('_')[-1]
        ind = collName.find(symbol) #合约第一个字母的位置
        strategyName = collName[:ind-1]
        # 从历史数据库中读取历史数据
        collection = dbClient[dbName][symbol]
        livedata = collection.find().sort('datetime',pymongo.ASCENDING)     
        df = pd.DataFrame(list(livedata))
        #print df
        try:
            df = df[df.TradingDay.isin(TradingDay)]       # 当前交易日
        except:
            continue
        if len(df) == 0:
            print 'no Data!!!'
            continue
        df = df[['close','open','high','low','datetime']]
        df.index = range(len(df))        
        #----------------------------------------------------------
        # 成交记录数据库 本地
        coll = dbClient_trade[tradeDbName][collName]
        tradedata = coll.find().sort('time',pymongo.ASCENDING)     
        tradedf = pd.DataFrame(list(tradedata))   # 插入到数据库的时间是按照时间顺序的
        trade_ind = None
        for i in range(len(tradedf.time)):
            if str(tradedf.time.tolist()[i]) <= str(df.datetime.tolist()[-1]):
                trade_ind = i
                #print trade_ind,'trade_ind'
            else:
                break
        if trade_ind >= 0:
            tradedf = tradedf.iloc[:trade_ind+1]
        else:
            print 'noTrade!!!'
            continue
        '''
        tradedf['date'] = [str(i.date()).replace('-','') for i in list(tradedf.time)]
        tradedf = tradedf[tradedf['date'].isin(TradingDay)]
        '''
        # 找到在df时间段内的交易信号，否则画图会报错
        tradeTimeRange = tradedf['time']
        tradeTimeRange = np.array([i.replace(second=0) for i in tradeTimeRange])
        if len(df) == 0:
            continue
        dftimeStart = df.iloc[0]['datetime']
        dftimeEnd   = df.iloc[-1]['datetime']
        if tradeTimeRange.tolist()[-1]<df.datetime[0]:
            continue
        ind1 = np.where(tradeTimeRange >= dftimeStart)[0][0]
        ind2 = np.where(tradeTimeRange <= dftimeEnd)[0][-1]
        tradedf = tradedf.iloc[ind1:ind2+1,:]
        tradedf.index = range(len(tradedf))
        # 将trade的time的秒换成零
        '''
        tradedf_time = []
        for i in range(len(tradedf)):
            tradedf_time.append(tradedf.iloc[i]['time'].replace(second = 0))
        tradedf['time'] = pd.Series(tradedf_time)  
        '''
        #---------------------------------------------------------- 
        # 找到成交记录index 找到交易点在成交记录中的位置
        close = df['close']
        indBuy = findIndex(tradedf,df,order='buy')
        indSell = findIndex(tradedf,df,order='sell')
        indShort = findIndex(tradedf,df,order='short')
        indCover = findIndex(tradedf,df,order='cover')
        #---------------------------------------------------------- 
        #画收盘价和买卖点
        fig = plt.figure()
        plt.title(collName)
        p = fig.add_subplot(1,1,1)
        p.plot(close,color='silver')
        #做多用红色，做空用绿色，开仓用正三角形，平仓用正方形
        p.scatter(indBuy, close[indBuy], s=70, marker='^', color="red",label = 'Buy')  
        p.scatter(indSell, close[indSell], s=70, marker='s', color="green",label = 'Sell')  
        p.scatter(indShort, close[indShort], s=70, marker='^', color="green",label = 'Short')  
        p.scatter(indCover, close[indCover], s=70,marker='s', color="red",label = 'Cover')  
        
        # 将坐标设置为时间
        timelabel = df['datetime']
        timelabelStr = [t.strftime("%m-%d %H:%M") for t in timelabel]
        # 将datetime类型转化为string类型
        numPoints = 10 # 需要标记的数据点的个数
        gap = len(df) / numPoints
        p.set_xticks(range(0,len(timelabel), gap))  
        p.set_xticklabels(timelabelStr[::gap])
        for tick in p.get_xticklabels():
                tick.set_rotation(7) # 旋转一定的角度，方便展示
        
        p.legend(loc='upper left')
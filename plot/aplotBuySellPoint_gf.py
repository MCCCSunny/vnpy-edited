# -*- coding: utf-8 -*-
'''
from __future__ import division

import os
import sys
reload(sys)  
sys.setdefaultencoding('utf8')  
curPath = os.getcwd()
gf_path = os.path.dirname(os.path.dirname(curPath)) # grandfather path
sys.path.append(gf_path)
'''

# 画出买入卖出点
#------------------------------------------------------------------

import pymongo
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import datetime
import talib

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
        while temp not in df.datetime.tolist():
            temp += datetime.timedelta(minutes=-1)
        if temp in df.datetime.tolist():
            ind = df.datetime.tolist().index(temp)
        index.append(ind)
    return index

#历史数据库
host = '192.168.1.107'    
port = 27017
dbName = 'VnTrader_1MIN_ALL_1106'
#交易日志在本地数据库
host1 = 'localhost'
port1 = 27017

tradeDbName = 'VnTrader_Trade_Db'
#tradeDbName = 'VnTrader_Trade_Db_JT'

#要显示的策略，合约
#collName = 'OBVStrategy_IC1801'
#collName = 'AtrRsiStrategy_rb1805'
#collName = 'DoubleMaStrategy_MA805'
collName = 'KkStrategy_IC1803'
#collName = 'BreakoutStrategy_a1801'
strategyName = collName[:collName.index('_')]
symbol = collName[collName.index('_')+1:]
#需显示的交易日列表
TradingDay = ['20171219']


# 从历史数据库中读取历史数据
dbClient = pymongo.MongoClient(host, port)
collection = dbClient[dbName][symbol]
livedata = collection.find().sort('datetime',pymongo.ASCENDING)     
df = pd.DataFrame(list(livedata))
df = df[ df.TradingDay.isin(TradingDay)]       # 当前交易日
df = df[['close','open','high','low','datetime']]
df.index = range(len(df))

#----------------------------------------------------------
# 成交记录数据库 本地
dbClient1 = pymongo.MongoClient(host1, port1)
coll = dbClient1[tradeDbName][collName]
tradedata = coll.find().sort('time',pymongo.ASCENDING)     
tradedf = pd.DataFrame(list(tradedata))   # 插入到数据库的时间是按照时间顺序的

# 找到在df时间段内的交易信号，否则画图会报错
tradeTimeRange = tradedf['time']
dftimeStart = df.iloc[0]['datetime']
dftimeEnd   = df.iloc[-1]['datetime']
ind1 = np.where(tradeTimeRange > dftimeStart)[0][0]
ind2 = np.where(tradeTimeRange < dftimeEnd)[0][-1]
tradedf = tradedf.iloc[ind1:ind2+1,:]
tradedf.index = range(len(tradedf))
# 将trade的time的秒换成零
tradedf_time = []
for i in range(len(tradedf)):
    tradedf_time.append(tradedf.iloc[i]['time'].replace(second = 0))
tradedf['time'] = pd.Series(tradedf_time)   
#---------------------------------------------------------- 
# 找到成交记录index 找到交易点在成交记录中的位置
close = df['close']
indBuy = findIndex(tradedf,df,order='buy')
indSell = findIndex(tradedf,df,'sell')
indShort = findIndex(tradedf,df,'short')
indCover = findIndex(tradedf,df,'cover')
#---------------------------------------------------------- 
#画收盘价和买卖点
fig = plt.figure()
plt.title(collName)
p = fig.add_subplot(1,1,1)
p.plot(close,color='silver')
#做多用红色，做空用绿色，开仓用正三角形，平仓用正方形
p.scatter(indBuy, close[indBuy], s=70, alpha = 0.5,marker='^', color="red",label = 'Buy')  
p.scatter(indSell, close[indSell], s=70, alpha = 0.5,marker='s', color="green",label = 'Sell')  
p.scatter(indShort, close[indShort], s=70, alpha = 0.5,marker='^', color="green",label = 'Short')  
p.scatter(indCover, close[indCover], s=70, alpha = 0.5,marker='s', color="red",label = 'Cover')  

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
        
'''
        
if strategyName == 'BreakoutStrategy':
    creat = 50
    closeout = 25
    openmax = np.array([np.nan]*creat)
    openmin = np.array([np.nan]*creat)
    for i in range(len(df.close)-creat):
        openmax = np.append(openmax, [max(df.close[i:i+creat])])
        openmin = np.append(openmin, [min(df.close[i:i+creat])])
    closemax = np.array([np.nan]*closeout)
    closemin = np.array([np.nan]*closeout)
    for i in range(len(df.close)-closeout):
        closemax = np.append(closemax, [max(df.close[i:i+closeout])])
        closemin = np.append(closemin, [min(df.close[i:i+closeout])])    
    p.plot(openmax,label ='openmax')
    p.plot(openmin,label = 'openmin')
    #p.plot(closemax,label ='closemax')
    #p.plot(closemin,label ='closemin')
    
    
p.legend(loc='upper left')
'''
'''
#---------------------------------------------------------- 
# 导入并计算策略的判断指标
strategyName = collName[:collName.index('_')]

if strategyName == 'AtrRsiStrategy':
    from vnpy.trader.app.ctaStrategy.strategy.strategyAtrRsi import AtrRsiStrategy
    """
    param = AtrRsiStrategy(ctaEngine, setting)
    atrLength = param.atrLength          # 计算ATR指标的窗口数   
    atrMaLength = param.atrMaLength        # 计算ATR均线的窗口数
    rsiLength = param.rsiLength           # 计算RSI的窗口数
    rsiEntry = param.rsiEntry           # RSI的开仓信号
    """
    atrLength = 22          # 计算ATR指标的窗口数   
    atrMaLength = 20        # 计算ATR均线的窗口数
    rsiLength = 5           # 计算RSI的窗口数
    rsiEntry = 16           # RSI的开仓信号
    
    atr = talib.ATR(np.array(df.high), np.array(df.low), np.array(df.close), atrLength)
    atrMa = talib.MA(np.array(atr),atrMaLength)
    rsi = talib.RSI(np.array(df.close), rsiLength)
    
    atrP = fig.add_subplot(3,1,2)
    atrP.plot(atr)
    atrP.plot(atrMa)
    
    rsiP = fig.add_subplot(3,1,3)
    rsiP.plot(rsi)
'''




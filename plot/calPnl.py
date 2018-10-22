# -*- coding: utf-8 -*-
"""
Created on Thu Nov 30 16:52:40 2017

@author: Molly
每天收盘后，从数据库里读取数据计算每天每个合约（单策略交易单合约）的盈亏情况
"""
import os
import sys
curPath = os.getcwd()
sys.path.append(curPath+'\\plot')
reload(sys)
sys.setdefaultencoding('utf8')
from pymongo import MongoClient
import pandas as pd
import datetime
import re
from TradeBase import TradeTimeFunc,readTradeDayList
    
def commission(symbolName, price, size, rate1, rate2):
    '''
    计算一手的手续费
    '''
    commission = 0
    feeList = ['fb','bb','pp','ag','rb','hc','ru','jd','cu','j','jm','i',\
            'pb','wr','hc','fu','bu','ru','if','ic','ih']
    lotList = ['a','b','c','cs','m', 'y', 'p', 'zc','al','zn','ni','au',\
            'l','v','wh','pm','cf','ta','oi','ri','ma','fg','rs','rm','zc','jr','lr',\
            'sf','sm','sn','tf','t']
    if symbolName.lower() in feeList:
        turnover = price *size  # 成交金额
        commission = turnover * rate1/10000.0                   # 按比例设置手续费成本
    elif symbolName.lower() in lotList:
        commission = rate2                       #按手数设置 乘以2收双边手续费
    return commission
    
def calPnl(host, port, host_history, port_history, Min_DB_NAME, Trade_DB_NAME, Settlement_DB_NAME, timestr, contract_Rate_Info):
    tradeDayList = readTradeDayList('plot\\TL_tradingDay.xlsx','20180101','20180130') # 从交易日列表excel中或许全部交易日信息 
    pnlDict = {}
    NoTradePnlDict = {} #用于保存今天没有交易的那些策略合约持仓收益
    dbClient = MongoClient(host, port)
    dbClient_history = MongoClient(host_history, port_history)
    db = dbClient[Trade_DB_NAME]
    #collectionNames = ['AtrRsiStrategy_IF1712']
    collectionNames = db.collection_names()    #获取所有正在交易的策略及相应的合约
    for oneColl in collectionNames:
        #print oneColl,'oneColl'
        pnl = 0
        symbol = oneColl.split('_')[-1] #合约名
        symbolName = re.match(r"([a-zA-Z]+)([0-9]*)", symbol).groups()[0] # 品种名称
        #strategy = oneColl[:-len(symbol)-1] #策略名
        #合约信息
        symbol_Rate_Info = contract_Rate_Info[(contract_Rate_Info.symbol == symbolName.upper())|(contract_Rate_Info.symbol == symbolName.lower())]     
        #读取今天的交易列表
        trade = dbClient[Trade_DB_NAME][oneColl]#每一个策略数据库的名字

        tradeTime = TradeTimeFunc(symbol)#该合约的交易时间
        eDay = datetime.datetime.strptime(timestr, '%Y%m%d').date() 
        sMin = datetime.datetime.strptime(tradeTime[0][0],'%H:%M').time() #结束时间
        eMin = datetime.datetime.strptime(tradeTime[-1][1],'%H:%M').time() #结束时间
        if len(tradeTime) == 2 or len(tradeTime) == 3:
            sDay = eDay
            sTime = datetime.datetime.combine(sDay,sMin)  #开始时间
            eTime = datetime.datetime.combine(eDay,eMin)
        elif len(tradeTime) == 4:
            sDay = tradeDayList[tradeDayList.index(timestr) - 1]   #开始日期
            sDay = datetime.datetime.strptime(sDay, '%Y%m%d').date() 
            sTime = datetime.datetime.combine(sDay,sMin)  #开始时间
            eTime = datetime.datetime.combine(eDay,eMin)
        tradeCursor = trade.find({'time':{'$gte':sTime,'$lte':eTime}}).sort('time') #取出今天的交易列表 
        trade_df = pd.DataFrame(list(tradeCursor)) 
        
        if len(trade_df) == 0:            
            profit = calPosPnl(db, oneColl, dbClient_history, Min_DB_NAME, symbol_Rate_Info, timestr)
            if profit == 0:
                 continue
            NoTradePnlDict[oneColl] = profit
            print u'%s合约今天没有交易'%(symbol)
            continue      
        #读取策略的每天开盘时的持仓情况,根据第一条成交记录来反推出昨天收盘时的持仓情况
        if trade_df.iloc[0].direction == u'空':
            pos0 = trade_df.iloc[0].pos + trade_df.iloc[0].volume
        elif trade_df.iloc[0].direction == u'多':
            pos0 = trade_df.iloc[0].pos - trade_df.iloc[0].volume
        
        '''
        pos = dbClient[Position_DB_NAME][strategy]
        posCursor = pos.find({'vtSymbol':symbol})
        pos_df = pd.DataFrame(list(posCursor))
        pos0 = pos_df.pos.values[0] #开盘时就持有的仓位
        '''
        #读取该合约的结算价格
        Settlement = dbClient[Settlement_DB_NAME][symbol]
        SettleCursor = Settlement.find({'TradingDay':timestr})
        Settle_df = pd.DataFrame(list(SettleCursor))
        if len(Settle_df) == 0:
            return
        settle = Settle_df.iloc[0].SettlePrice
        preSettle = Settle_df.iloc[0].preSettle
        pnl += (settle - preSettle)* pos0 * symbol_Rate_Info['TradeUnit'].values[0]#开仓时就持有的仓位的盈亏
        #print pnl
        if len(trade_df) == 0:
            pnlDict[oneColl] = pnl
        else:
            for i in range(len(trade_df)):
                if trade_df.iloc[i].direction == u'多':
                    pnl += (settle - trade_df.iloc[i].price)*trade_df.iloc[i].volume * symbol_Rate_Info['TradeUnit'].values[0]
                    commission1 = commission(symbolName, trade_df.iloc[i].price, symbol_Rate_Info['TradeUnit'].values[0], symbol_Rate_Info['ratefee'].values[0], symbol_Rate_Info['ratelot'].values[0])
                    pnl -= trade_df.iloc[i].volume * commission1
                elif trade_df.iloc[i].direction == u'空':
                    pnl += (trade_df.iloc[i].price - settle)*trade_df.iloc[i].volume * symbol_Rate_Info['TradeUnit'].values[0]
                    commission1 = commission(symbolName, trade_df.iloc[i].price, symbol_Rate_Info['TradeUnit'].values[0], symbol_Rate_Info['ratefee'].values[0], symbol_Rate_Info['ratelot'].values[0])                     
                    pnl -= trade_df.iloc[i].volume * commission1                    
            pnlDict[oneColl] = pnl
    return pnlDict,NoTradePnlDict
    #print 'pnlDict',pnlDict

def calPosPnl(db, oneColl, dbClient, Min_DB_NAME, symbol_Rate_Info, timestr):
    '''
    计算今天没有交易但是有持仓的那些合约的收益
    '''
    #最后一条交易的持仓数据为0，则表示今天无持仓
    pos = pd.DataFrame(list(db[oneColl].find())).iloc[-1].pos
    if pos == 0:
        return 0
    else:
        #不用考虑持仓从什么时候开始，我只计算今天持仓的盈亏情况        
        symbol = oneColl.split('_')[-1]
        db = dbClient[Min_DB_NAME][symbol]
        data_his = pd.DataFrame(list(db.find({'TradingDay': timestr}).sort('datetime'))) 
        print len(data_his),'data_his'
        Price1 = data_his.iloc[0].close
        Price2 = data_his.iloc[-1].close
        pnl = pos * (Price2 - Price1) * symbol_Rate_Info['TradeUnit'].values[0]
        return pnl
            
if __name__ == '__main__':
    import os
    host = 'localhost'
    port = 27017
    host_history = '192.168.1.102'
    port_history = 27017
    Min_DB_NAME = 'VnTrader_1MIN_ALL_1106'
    Trade_DB_NAME = 'VnTrader_Trade_Db_Molly' #molly 成交信息
    Settlement_DB_NAME = 'VnTrader_settle_Db_Molly' #Molly 保存前一日结算价和最新结算价
    #Account_DB_Name = 'VnTrader_account_Molly'#Molly 保存账户资金情况
    #Position_DB_NAME = 'VnTrader_Position_Db_Molly'
    
    timestr = '20180105'
    contractRatePath = 'F:\\study\\quantitative trading\\symbol_info\\contractRate.csv'
    contract_Rate_Info = pd.read_csv(contractRatePath,encoding = 'gbk')
    pnlDict, NoTradePnlDict = calPnl(host, port, host_history, port_history, Min_DB_NAME, Trade_DB_NAME, Settlement_DB_NAME, timestr, contract_Rate_Info)   
    print 'pnlDict',pnlDict,'NoTradePnlDict',NoTradePnlDict
    if pnlDict is None:
        print '今日策略无交易'
        os._exit(0)
    pnlAll_trade = 0   
    pnlAll_notrade = 0
    for i in range(len(pnlDict)):
        pnlAll_trade += pnlDict[pnlDict.keys()[i]]
    for i in range(len(NoTradePnlDict)):
        pnlAll_notrade += NoTradePnlDict[NoTradePnlDict.keys()[i]]
    pnl_All = pnlAll_trade + pnlAll_notrade
    print pnlAll_trade, pnlAll_notrade, pnl_All
    #将每天计算的结果插入到数据库中
    dbClient = MongoClient(host, port)  
    db = dbClient['pnl']
    for one in pnlDict.keys():        
        print one,pnlDict[one]
        d = {'TradingDay':timestr,
             'pnl':pnlDict[one],
             'trade':True,
             'localTime':datetime.datetime.now()
             }
        #flt = {'TradingDay':timestr}
        collection = db[one]
        collection.insert_one(d)
    print u'tradePnl 插入完毕'    
    for one1 in NoTradePnlDict.keys():
        d = {'TradingDay':timestr,
             'pnl':NoTradePnlDict[one1],
             'trade':False,
             'localTime':datetime.datetime.now()
             }
        collection = db[one1]
        collection.insert_one(d)
    print u'NoTradePnl 插入完毕'        
# -*- coding: utf-8 -*-
"""
Created on Sat Dec 28 18:42:01 2019

@author: Prashant
"""



import pandas as pd
import time
import datetime
import numpy as np

import pymysql

import sqlalchemy

def getDFforCloseTime(TradeTime) :
    engine = getconn()

    #Tradetime should be 14:55 
    query =  "SELECT *  FROM bnfonemindata where TradeTime = '14:56:00'" 
    df = pd.read_sql_query(query, engine)
    return df

def getDF() :
    engine = getconn()
    query = "select * from bnfonemindata where tradedate between '2016-10-13' AND '2029-10-12'"

    #query = "select * from onemindata where (Tradetime = '14:56:00') or (Tradetime = '15:15:00') or((Volume >25000) and (Tradetime between '09:35:00' and '14:55:00')) order by TradeDate"
    df = pd.read_sql_query(query, engine)
    return df


def getTradeDates() :
    engine = getconn()
    query =  "SELECT distinct(TradeDate) from bnfonemindata where TradeDate between '2016-10-13' AND '2029-10-12'" 
    df = pd.read_sql_query(query, engine)
    return df

def getconn():
    conn = sqlalchemy.create_engine('mysql+pymysql://root:admin2@localhost:3306/analysisschema') 
    return conn

def openMove() :
    dfvolfull = getlabelDeltas()
    dftradedate = getTradeDates()
    
    dfhigh = pd.DataFrame(columns= ['TradeDate','high16', 'highLater', 'delta'])
    dflow = pd.DataFrame(columns= ['TradeDate','low16', 'lowLater', 'delta'])
    
    hindex = 0
    lindex = 0
    tindex = 0
    for index, row in dftradedate.iterrows():
        tindex = tindex + 1
        dfday = dfvolfull[dfvolfull['TradeDate'] == row['TradeDate']]
        h16 = dfday.iloc[0]['High']
        h17 = dfday.iloc[1]['High']
        op16 = dfday.iloc[1]['Open']
        l16 = dfday.iloc[0]['Low']
        l17 = dfday.iloc[1]['Low']
        
        
        if(h17>h16) :
            hindex = hindex + 1
            print("17 higher: " , hindex)
            for x in range(2, 45):
                if (dfday.iloc[x]['Low'] < op16):
                    #print(x," , ", dfday.iloc[x]['Low']," , ", dfday.iloc[x]['delta'])
                    dflow.loc[len(dflow)] = [row['TradeDate'],l16,dfday.iloc[x]['Low'], dfday.iloc[x-1]['delta']]
                    break
                
        
        if(l17<l16) :
            lindex = lindex + 1
            print("17 lower: " , lindex)
            for x in range(2, 45):
                if (dfday.iloc[x]['High'] > op16):
                    #print(x," , ", dfday.iloc[x]['High']," , ", dfday.iloc[x]['delta'])
                    dfhigh.loc[len(dfhigh)] = [row['TradeDate'],h16,dfday.iloc[x]['High'], dfday.iloc[x-1]['delta']]
                    break
        
        
        
    
    t = time.localtime()
    current_time = time.strftime("%H%M%S", t)
    
    print("total sample ", tindex)
    
    dflow.to_csv(current_time + 'openLowBroken.csv', encoding='utf-8', index=True)
    dfhigh.to_csv(current_time + 'openHighBroken.csv', encoding='utf-8', index=True)
    

def polulateAverage() :
    dfvolfull = getlabelDeltas()
    dftradedate = getTradeDates()
    
    dfcsv = pd.DataFrame(columns= ['TradeDateTime','Average'])
  
    for index, row in dftradedate.iterrows():
        
        #print(row['TradeDate'])
        dfday = dfvolfull[dfvolfull['TradeDate'] == row['TradeDate']]
        dfday['Sno'] = np.arange(len(dfday)) + 1
        
        dfday['Average'] = dfday['Close'].cumsum(axis = 0)/dfday['Sno']
        dfday['Average'] = dfday['Average']//10
        
        dfcsv = pd.concat([dfcsv, dfday], join="inner")
    
    return dfcsv
    #dfcsv.to_csv( 'dayaverage.csv', encoding='utf-8', index=True)

def buysellAfterMoveAwayFromOpen() :
    dfvolfull = getlabelDeltas()
    dftradedate = getTradeDates()
    
    dfcsv = pd.DataFrame(columns= ['TradeDate','buyTime', 'buyReturn'])
  
    for index, row in dftradedate.iterrows():
        
        #print(row['TradeDate'])
        dfday = dfvolfull[dfvolfull['TradeDate'] == row['TradeDate']]
        dfday['Sno'] = np.arange(len(dfday)) + 1
        
        dfday['Average'] = dfday['Close'].cumsum(axis = 0)/dfday['Sno']
        
        o = dfday.iloc[0]['Close']//10
        
        buyat = o-15
        sellat = o +15

        
        dfb = dfday.loc[dfday['Close2'] == buyat]
        if(len(dfb)>0) :
            locbuy = dfb.iloc[0]['TradeTime']
            retbuy = dfb.iloc[0]['delta']
            dfcsv.loc[len(dfcsv)] = [row['TradeDate'],locbuy,retbuy]
    
        dfb = dfday.loc[dfday['Close2'] == sellat]
        if(len(dfb)>0) :
            locbuy = dfb.iloc[0]['TradeTime']
            retbuy = -1* dfb.iloc[0]['delta']
            dfcsv.loc[len(dfcsv)] = [row['TradeDate'],locbuy,retbuy]
    
    
    
     
    #dfday['C2O'] = dfday['C2C'] + dfday[['Open']]-dfday[['Close']]
    
    dfcsv.to_csv( 'dayOHLCV.csv', encoding='utf-8', index=False)

    #print(dfcsv['C2O'].describe())
    #dbWrite(dfcsv)


def getlabelDeltas() :
    
    """
    Labels all rows with +/- 50
    1. Divide all rows by 50
    2. Take mode. Group into 5 modes.
    3. FInd next mode.
    """   
       
    dfvolfull = getDF()

    
    
   
    dfvolfull['TradeTime']  = pd.to_datetime(dfvolfull['TradeTime']).dt.strftime('%H:%M:%S')

    
    dfvolfull['TradeDate'] = pd.to_datetime(dfvolfull['TradeDate'], format='%Y-%m-%d')
 
    #exDate = datetime.datetime.strptime('2017-01-01' , '%Y-%m-%d').date()
    #dfvolfull = dfvolfull[dfvolfull['TradeDate'] > exDate]
    print(len(dfvolfull))
    
    #dfvolfull["TradeDateTime"] = dfvolfull["TradeDate"] + dfvolfull["TradeTime"]
    
    dfvolfull['Close2'] = dfvolfull['Close']//10
    dfvolfull['mod'] = dfvolfull['Close2']%10
 
    df1 = getDfForMod(1,dfvolfull)
    df2 = getDfForMod(2,dfvolfull)
    df3 = getDfForMod(3,dfvolfull)
    df4 = getDfForMod(4,dfvolfull)
    df5 = getDfForMod(0,dfvolfull)
    dffinal = df3
    dffinal = pd.concat([df1, df2, df3,df4, df5],ignore_index=True)
    #print(dffinal['TradeTime'])
    
    dffinal.set_index('TradeDateTime', inplace=True, verify_integrity=False)
    dffinal = dffinal.sort_index()
    t = time.localtime()
    current_time = time.strftime("%H%M%S", t)
    
    
    #dbWrite(dffinal)
    #dffinal.to_csv(current_time + 'sellfornextday.csv', encoding='utf-8', index=True)
    return dffinal
    
    
def getDfForMod(mod, dfvolfull) : 
    modplus = mod +5
    dfvol = dfvolfull[(dfvolfull['mod']==mod) | (dfvolfull['mod']==modplus) ]
    #print (dfvol['mod'])
    dfvol['delta'] = (dfvol[['Close2']]-dfvol[['Close2']].shift()) * 10 
    dfvol['delta'] = dfvol['delta'].shift(-1)
    dfvol['delta'] = dfvol['delta'].replace(to_replace=0, method='bfill')
    
    dfvol['delta'] = dfvol['delta'].apply(lambda x: 50 if x > 0 else -50)
    dfvol['book'] = (dfvol[['mod']] != dfvol[['mod']].shift()).any(axis=1)
    #dfvol['valuesum'] = dfvol['value'].cumsum()
    #dfvol['mod']
    """
    del dfvol['Close']
    del dfvol['Close2']
    del dfvol['Open']
    del dfvol['mod'] 
    """

    
   
    #dfvol.to_csv(current_time + 'buyfuturedip.csv', encoding='utf-8', index=False)
    return dfvol
     
def poluateDayOHLCVTable() :
    dfvolfull = getDF()
    dftradedate = getTradeDates()
    
    dfcsv = pd.DataFrame(columns= ['TradeDate','Open', 'High', 'Low', 'Close', 'Volume', 'Range','OtoC'])
  
    for index, row in dftradedate.iterrows():
        
        #print(row['TradeDate'])
        dfday = dfvolfull[dfvolfull['TradeDate'] == row['TradeDate']]
       

        o = dfday.iloc[0]['Open']
        h = dfday['High'].max()
        l = dfday['Low'].min()
        c = dfday.iloc[-1]['Close']
        v= dfday['Volume'].sum()
        r= (h-l)//1
        o2c = abs((c-o)//1)
        dfcsv.loc[len(dfcsv)] = [row['TradeDate'],o,h,l,c,v,r, o2c]
    
    dfday = dfday.sort_values('TradeDate')
    dfcsv['CtoC'] = (dfcsv[['Close']]-dfcsv[['Close']].shift()).abs()
    
     
    #dfday['C2O'] = dfday['C2C'] + dfday[['Open']]-dfday[['Close']]
    
    dfcsv.to_csv( 'dayOHLCV.csv', encoding='utf-8', index=False)
    print(dfcsv['OtoC'].describe())
    print(dfcsv['Range'].describe())
    print(dfcsv['Volume'].describe())
    print(dfcsv['CtoC'].describe())
    #print(dfcsv['C2O'].describe())
    #dbWrite(dfcsv)       
 
def addAveragetoDelta() :
    
    df1 = getlabelDeltas()
    df2 = polulateAverage()
     
    df3 = pd.concat([df1, df2], axis=1)
    t = time.localtime()
    current_time = time.strftime("%H%M%S", t)
    
    df3= df3[df3['TradeTime'] > '09:20:00']
    
    del df3['Close']
    del df3['Low']
    del df3['High']
    del df3['Open']
    
    df3['unitary'] = 1
 
    df3.to_csv(current_time + 'deltaavg.csv', encoding='utf-8', index=True)
    return df3
    
def dbWrite(data) :
    pymysql.converters.encoders[np.float64] = pymysql.converters.escape_float
    pymysql.converters.conversions = pymysql.converters.encoders.copy()
    pymysql.converters.conversions.update(pymysql.converters.decoders)
    engine = sqlalchemy.create_engine('mysql+pymysql://root:admin2@localhost:3306/analysisschema')

    #connection = engine.connect()
    #data.to_sql('ExpiryData', con=engine)
    data.to_sql(name='DeltaForBNF', con=engine, if_exists = 'replace', index=False)
    #metadata = sqlalchemy.MetaData()
    #expiryData = sqlalchemy.Table('ExpiryData', metadata, autoload=True, autoload_with=engine)

    #print(repr(metadata.tables['ExpiryData']))
 
  

    
    
      
if __name__=="__main__":
    #print(getStrike("\'2019/07/26\'"))
    
    t = time.localtime()
    current_time = time.strftime("%H:%M:%S", t)
    
    openMove()
    t = time.localtime()
    current_time1 = time.strftime("%H:%M:%S", t)
    print(current_time)
    print(current_time1)
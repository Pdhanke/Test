# -*- coding: utf-8 -*-
"""
Created on Sat Dec 28 18:42:01 2019

@author: Prashant
"""



import pandas as pd
import time


import sqlalchemy



def getDFforDateTime(TradeTime) :
    conn = getconn() 

    #query = "SELECT Currenttimeinmslong, Lasttradedtime, LastTradedPrice, LastTradedVolume from tickdataaug where tradingSymbol = \"BANKNIFTY19AUGFUT\" and currenttimeinmslong between 1565932352493 and 1565949599581"
    query =  "SELECT Strike, OfType, Close, TradeDateTime, TradeDate  FROM OptionBnifty WHERE TradeTime = " + TradeTime 
    df = pd.read_sql_query(query, conn)
    return df

def getDFforTime(TradeDateTime) :
    conn = getconn() 
    #query = "SELECT Currenttimeinmslong, Lasttradedtime, LastTradedPrice, LastTradedVolume from tickdataaug where tradingSymbol = \"BANKNIFTY19AUGFUT\" and currenttimeinmslong between 1565932352493 and 1565949599581"
    query =  "SELECT Strike, Oftype, Close, TradeDateTime, TradeDate, TradeTime  FROM OptionBnifty WHERE TradeDateTime = " + TradeDateTime 
    df = pd.read_sql_query(query, conn)
    return df

def getDFforDateTimeStrike(TradeDateTime, Strike) :
    conn = getconn() 
    #query = "SELECT Currenttimeinmslong, Lasttradedtime, LastTradedPrice, LastTradedVolume from tickdataaug where tradingSymbol = \"BANKNIFTY19AUGFUT\" and currenttimeinmslong between 1565932352493 and 1565949599581"
    query =  "SELECT Strike, OfType, Close, TradeDateTime  FROM OptionBnifty WHERE TradeDateTime = " + TradeDateTime + " and Strike ="+ Strike
    df = pd.read_sql_query(query, conn)
    return df

def getDFforTimeStrike(TradeTime, Strike) :
    conn = getconn() 
    #query = "SELECT Currenttimeinmslong, Lasttradedtime, LastTradedPrice, LastTradedVolume from tickdataaug where tradingSymbol = \"BANKNIFTY19AUGFUT\" and currenttimeinmslong between 1565932352493 and 1565949599581"
    query =  "SELECT Strike, OfType, Close, TradeDateTime  FROM OptionBnifty WHERE TradeTime = " + TradeTime + " and Strike ="+ Strike
    df = pd.read_sql_query(query, conn)
    return df

def getDFforDateStrike(TradeDate, Strike) :
    conn = getconn() 
    #query =  "SELECT Strike, Oftype, Close, TradeDateTime, TradeTime, TradeDate FROM OptionBnifty WHERE TradeDate =" + TradeDate + " and Strike ="+ Strike
    query =  "SELECT Strike, Oftype, Close, TradeDateTime, TradeTime, TradeDate FROM OptionBnifty WHERE TradeDate ='" + TradeDate + "' and Strike ='"+ Strike +"'"
    #print(query)
    df = pd.read_sql_query(query, conn)
    return df

def getDFforAllDateStrikes() :
    conn = getconn() 
    #query =  "SELECT Strike, Oftype, Close, TradeDateTime, TradeTime, TradeDate FROM OptionBnifty WHERE TradeDate =" + TradeDate + " and Strike ="+ Strike
    #query =  "SELECT Strike, Oftype, Close, TradeDateTime, TradeTime, TradeDate FROM OptionBnifty WHERE Strike in (" + Strikes + ")"
    query =  "SELECT Strike, Oftype, Close, TradeDateTime, TradeTime, TradeDate FROM OptionBnifty"

    print(query)
    df = pd.read_sql_query(query, conn)
    return df

def getTradeDates() :
    conn = getconn()   
    query =  "SELECT DISTINCT (TradeDate) from OptionBnifty where TradeDate BETWEEN '2015-10-24' AND '2019-12-29'" 
    df = pd.read_sql_query(query, conn)
    return df

def getconn():
    conn = sqlalchemy.create_engine('mysql+pymysql://root:admin2@localhost:3306/analysisschema') 
    return conn

def populateAverageValues(dfWithMinStrikeForEachDate) :
    dfcsv = pd.DataFrame(columns= ['TradeDate','maxsum', 'minsum', 'maxtime', 'mintime'])
    

    dft = getDFforAllDateStrikes()
    
    for index, row in dfWithMinStrikeForEachDate.iterrows():

        
        df = dft[dft['TradeDate'] == row['TradeDate']]
        df = df[df['Strike'] == row['Strike']]

        #print(df)
        df.reset_index(drop=True, inplace=True)
        #print(df)
        dfce = df[df['Oftype'] == 'CE']
        #print (dfce)
        dfpe = df[df['Oftype'] == 'PE']

        dfmerge1 = dfce.merge(dfpe,on='TradeDateTime',
                   how='inner')
    
    
        dfmerge1['sum'] = dfmerge1['Close_x'] + dfmerge1['Close_y']
        

        maxsum = dfmerge1['sum'].max()
        dfmergemax = dfmerge1[dfmerge1['sum'] == dfmerge1['sum'].max()]
        if(maxsum>200) :
            maxsum = 200
        
        
        #print(dfmergemax.iloc[0]['TradeDateTime'])
        #print(dfmergemax.iloc[0]['TradeDateTime_y'])
        minsum = dfmerge1['sum'].min()
        dfmergemin = dfmerge1[dfmerge1['sum'] == dfmerge1['sum'].min()]
        if(minsum>200) :
            minsum = 200
        #print(dfmergemin.iloc[0]['TradeDateTime'])
        #print(dfmergemin.iloc[0]['TradeDateTime_y'])
        
        dfcsv.loc[len(dfcsv)] = [row['TradeDate'], maxsum, minsum, dfmergemax.iloc[0]['TradeDateTime'], dfmergemin.iloc[0]['TradeDateTime']]
    
    dfmerge = dfWithMinStrikeForEachDate.merge(dfcsv,on='TradeDate',
                   how='inner')  
    #dfmerge['weekday']=dfmerge['TradeDate'].weeekday()
    dfmerge['weekday'] = dfmerge['TradeDate'].apply(lambda x: x.weekday())
    
    t = time.localtime()
    current_time = time.strftime("%H%M%S", t)
    dfmerge.to_csv(current_time + 'sellfornextday.csv', encoding='utf-8', index=False)
    
def populateAverageValuesForDays(dfWithMinStrikeForEachDate) : #get average values of on-strike cepe sum for all time 
    
    dfavg = pd.DataFrame(columns= ['Strike_x', 'sum','weekday','TradeDateTime', 'TradeTime_x', 'TradeDate_x' ])
    

    dft = getDFforAllDateStrikes()
    
    for index, row in dfWithMinStrikeForEachDate.iterrows():

        
        df = dft[dft['TradeDate'] == row['TradeDate']]
        df = df[df['Strike'] == row['Strike']]

        #print(df)
        df.reset_index(drop=True, inplace=True)
        #print(df)
        dfce = df[df['Oftype'] == 'CE']
        #print (dfce)
        dfpe = df[df['Oftype'] == 'PE']

        dfmerge1 = dfce.merge(dfpe,on='TradeDateTime',
                   how='inner')
    
    
        dfmerge1['sum'] = dfmerge1['Close_x'] + dfmerge1['Close_y']
        leng = len(dfmerge1)
        print(leng)
        if(leng>40):
            t = time.localtime()
            current_time = time.strftime("%H%M%S", t)
            dfmerge1.to_csv(current_time + 'sellfornextday.csv', encoding='utf-8', index=True)
        if(leng<40):
            beginsum = dfmerge1.iloc[19]['sum']/100
            dfmerge1['sum'] = dfmerge1['sum']/beginsum
            
            dfmerge1['weekday'] = dfmerge1['TradeDate_x'].apply(lambda x: x.weekday())
            csv2 = dfmerge1[['Strike_x', 'sum','weekday', 'TradeTime_x' ]]
            #print(csv2)
            dfavg = pd.concat([dfavg, csv2], ignore_index=True)
            #print(dfmergemin.iloc[0]['TradeDateTime'])
        #print(dfmergemin.iloc[0]['TradeDateTime_y'])

    dfavg = dfavg.groupby(['TradeTime_x', 'weekday']).mean()
    
    t = time.localtime()
    current_time = time.strftime("%H%M%S", t)
    dfavg.to_csv(current_time + 'sellfornextday.csv', encoding='utf-8', index=True)
    
def dfWithMinStrikeForEachDate() :
    dfcsv = pd.DataFrame(columns= ['TradeDate','Strike', 'beginsum', 'endsum', 'diff'])
    
    dftradedate = getTradeDates()

    
    
    
    dfstart = getDFforDateTime( "\'09:20:00\'")

    
    dfend = getDFforDateTime( "\'15:20:00\'")
 
    
    
    count = 0
    tradedcount=0

   
    for index, row in dftradedate.iterrows():
        #print ("\"\\\'" + row['tradedate'] + "\\\'\"")
        formatedDate =  row['TradeDate']
        
        
        
        dfmin = (getdfWithMinSumRows(dfstart,formatedDate))
        #print(dfmin['Strike'])
        #print(dfmin['sum'])
        #print ("Strike=", dfmin['Strike'], dfmin['Close_x'], dfmin['Close_y'], dfmin['sum'])
        #print(formatedDate)
        beginsum = 5000;
        
        if not(dfmin.empty) :
            beginsum = dfmin.iloc[0]['sum']
            
        if(beginsum <1000 and beginsum>20) :
                  
            endsum =  getdfWithENDSumRows(dfend, formatedDate, dfmin.iloc[0]['Strike'])
            #print(beginsum)
            if(endsum > 30*beginsum) :
                endsum = 30*beginsum

            diff = beginsum - endsum
            if(diff>100) :
                diff = 100
            if(diff<-100) :
                diff = -100
           
            tradedcount= tradedcount + 1;
            #add valid entry to df for csv
            dfcsv.loc[len(dfcsv)] = [row['TradeDate'],dfmin.iloc[0]['Strike'], beginsum, endsum, diff] 
        count = count + 1;
        
        print("----------")
        
    
    #print(count)
    #print(tradedcount)

    #print(dfcsv)
    t = time.localtime()
    current_time = time.strftime("%H%M%S", t)
    #dfcsv['TradeDate'] =  pd.to_datetime(dfcsv['TradeDate'], format = '%Y-%m-%d')
    #populateAverageValues(dfcsv)
    populateAverageValuesForDays(dfcsv)
    #dfcsv.to_csv(current_time + 'sellfornextday.csv', encoding='utf-8', index=False)
        
    
        
        

def getdfWithMinSumRows(dfstart, TradeDate) :
    #df = getDFforDateTime("\'2019/07/26\'", "\'09:30:00\'")
    #print(dfstart)
    #print(TradeDate)
    df = dfstart[dfstart['TradeDate'] == TradeDate]
    #print(df)
    dfce = df[df['OfType'] == 'CE']
    dfpe = df[df['OfType'] == 'PE']
    dfmerge = dfce.merge(dfpe,on='Strike',
                   how='inner')
    
    
    dfmerge['sum'] = dfmerge['Close_x'] + dfmerge['Close_y']
    
    #print(dfmerge)
    
    return (dfmerge[dfmerge['sum'] == dfmerge['sum'].min()])

 
def getdfWithENDSumRows(dfend, TradeDate, Strike) :
    #df = getDFforDateTime("\'2019/07/26\'", "\'09:30:00\'")
    ##print(Strike)
    #df = getDFforDateTimeStrike(TradeDate, "\'15:20:00\'",str(Strike))
    #df = dfend[dfend['TradeDate'] == TradeDate and dfend['Strike'] == Strike ]
    df = dfend[dfend['TradeDate'] == TradeDate]
    df = df[df['Strike'] == Strike]
    #print(df)
    
    dfce = df[df['OfType'] == 'CE']
    
    sumcp = 50000
    dfpe = df[df['OfType'] == 'PE']
    if not(dfce.empty) :
        if not(dfpe.empty):
            sumcp= dfce.iloc[0]['Close'] + dfpe.iloc[0]['Close']
    return sumcp
    
    
      
if __name__=="__main__":
    #print(getStrike("\'2019/07/26\'"))
    
    t = time.localtime()
    current_time = time.strftime("%H:%M:%S", t)
    
    dfWithMinStrikeForEachDate()
    t = time.localtime()
    current_time1 = time.strftime("%H:%M:%S", t)
    print(current_time)
    print(current_time1)
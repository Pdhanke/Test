# -*- coding: utf-8 -*-
"""
Created on Fri Jan 17 17:32:52 2020

@author: Prashant
"""

# -*- coding: utf-8 -*-
"""
Created on Mon Dec 23 13:38:05 2019

@author: Prashant
"""

import os
import pandas as pd
import datetime



import pymysql

import numpy as np

import sqlalchemy



        


def iterateDir() : 

    rootPath = r"C:\Users\Prashant\workspace\pythonProjects\data\weeklydata\nifty"
       
    

    for dirpath, dirnames, filenames in os.walk(rootPath):

        for file in filenames:
         filepath = dirpath +os.sep+file
         if filepath.endswith('csv'):
             print(filepath)
             saveDatefromCSV(filepath)
             
             



def logic(index):
    if index % 3 == 0:
       return False
    return True




    
    #data.to_sql(name='ExpiryData', con=engine, if_exists = 'append', index=False)

def saveDatefromCSV(filepath) : 

 
 

     
     
    #data = pd.read_csv(filepath, names=['Strike','TradeDate','TradeTime','Open','High','Low','Close','Volume'], skiprows= lambda x: logic(x))
    data = pd.read_csv(filepath, names=['Strike','TradeDate','TradeTime','Open','High','Low','Close','Volume'])
   
    
    

   
    wrongSymbol = data['Strike'][0]  #BANKNIFTYWK28100CE
    
    #Determine whether PE , CE  or FUT
     #Determine whether PE , CE  or FUT
    typeOfSymbol = "FUT"
    if("CE" in wrongSymbol) :
        typeOfSymbol = "CE"
    if("PE" in wrongSymbol) :
        typeOfSymbol = "PE"
    data['Oftype']= typeOfSymbol
    
    if(wrongSymbol.endswith('0')):
        strike=float(wrongSymbol[-5:])
    else :
        strike=float(wrongSymbol[-7:-2])
    data['Strike'] = strike
    

    
    #Expiry date is the last row date
    exPiryDate = data.iloc[-1][1]
    
    #print(exPiryDate)
    exPiryDate = exPiryDate.replace("/","-")

    if(exPiryDate.find('-')<3) :
        exPiryDate = datetime.datetime.strptime(exPiryDate, '%d-%m-%Y').strftime('%Y-%m-%d')
    print(exPiryDate)
    data['Expiry']= exPiryDate
    

    data = data[data.TradeTime.str.endswith('0')]
    print(data)
    
    #getting only last 7 days data

    
    exDate = datetime.datetime.strptime(exPiryDate , '%Y-%m-%d').date()
    end_date = exDate - datetime.timedelta(days=7)
    data['TradeDate'] =  pd.to_datetime(data['TradeDate'])
    data = data[(data['TradeDate'] > end_date)]
    
    data['TradeDate'] = pd.to_datetime(data["TradeDate"]).dt.strftime('%Y-%m-%d')
    data['TradeDateTime'] = data['TradeDate'] + " "+ data['TradeTime'] + ":00"
    
    
    
   
      
    
    #Inserting all the data of the file
    minvalue = data['Close'].min()
    maxvalue = data['Close'].max()
    
    if(minvalue<500 and maxvalue >5):
        data = data[(data != 0).all(1)]
        dbWrite(data)
    
def dbWrite(data) :
    pymysql.converters.encoders[np.float64] = pymysql.converters.escape_float
    pymysql.converters.conversions = pymysql.converters.encoders.copy()
    pymysql.converters.conversions.update(pymysql.converters.decoders)
    engine = sqlalchemy.create_engine('mysql+pymysql://root:admin2@localhost:3306/analysisschema')

    #connection = engine.connect()
    #data.to_sql('ExpiryData', con=engine)
    data.to_sql(name='OptionBNifty', con=engine, if_exists = 'append', index=False)
    #metadata = sqlalchemy.MetaData()
    #expiryData = sqlalchemy.Table('ExpiryData', metadata, autoload=True, autoload_with=engine)

    #print(repr(metadata.tables['ExpiryData']))
 
    #data.to_sql(expiryData, connection)
    
    


if __name__=="__main__":
    iterateDir()
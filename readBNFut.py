# -*- coding: utf-8 -*-
"""
Created on Mon Dec 23 13:38:05 2019

@author: Prashant
"""

import re
import fnmatch,os, stat, shutil
import pandas as pd
import datetime
import zipfile



import pymysql

import numpy as np

import sqlalchemy



def numInString() :

    s = 'CSV 03-07-19 to 18-07-19 (Expiry Day)'
    num = re.findall('\d+', s)
    print(num[0])    

    s2 = re.findall('\d\d-\d\d-\d\d', s)
    s3 = s2[1].replace("-","\\")
    print (s3)
    
def unzipall() : 
    print('hi')
    
    rootPath = r"C:\Users\Prashant\workspace\pythonProjects\oneminFut\2016\jul"
    pattern = 'BANKNIFTY_F1.*'
    for root, dirs, files in os.walk(rootPath):
        for filename in fnmatch.filter(files, pattern):
            print(os.path.join(root, filename))
            saveDatefromCSV(os.path.join(root, filename))

            #os.remove(os.path.join(root, filename))
            #zipfile.ZipFile(os.path.join(root, filename)).extractall(os.path.join(root, os.path.splitext(filename)[0]))

def renameFolder(rootPath, oldName, newName) :
    os.rename(os.path.join(rootPath, oldName),
                      os.path.join(rootPath, newName))
    
    



def renameFolders() : 

    rootPath = r"C:\Users\Prashant\workspace\pythonProjects\data\oneminFut\2016"
       
    pattern = 'NIFTY_F1.*'

    for dirpath, dirnames, files in os.walk(rootPath):

        for filename in fnmatch.filter(files, pattern):
            filepath = dirpath +os.sep+filename
            if filepath.endswith('txt'):
                print(filepath)
                saveDatefromCSV(filepath)
             
             





    
    #data.to_sql(name='ExpiryData', con=engine, if_exists = 'append', index=False)

def saveDatefromCSV(filepath) :     
     
    data = pd.read_csv(filepath, names=['TradingSymbol','TradeDate','TradeTime','Open','High','Low','Close','Volume', 'OI'])
   
 
   

    #exPiryDate = datetime.datetime.strptime(exPiryDate, "%d/%m/%Y").strftime("%Y/%m/%d")
    
    data['TradingSymbol'] = 'NIFTYFUT'
    #data['TradeDate'] = pd.to_datetime(data["TradeDate"]).dt.strftime('%Y-%m-%d')
    data['TradeTime'] = pd.to_datetime(data["TradeTime"]).dt.strftime('%H:%M:%S')
    
    #data['TradeDateTime'] = data['TradeDate'] + " " + data['TradeTime']
    data['TradeDate'] = pd.to_datetime(data['TradeDate'], format='%Y%m%d')
    data['TradeDate'] = pd.to_datetime(data["TradeDate"]).dt.strftime('%Y-%m-%d')

    
    data['TradeDateTime'] = data['TradeDate'] + " "+ data['TradeTime'] 
   
    dbWrite(data)
    
def dbWrite(data) :
    pymysql.converters.encoders[np.float64] = pymysql.converters.escape_float
    pymysql.converters.conversions = pymysql.converters.encoders.copy()
    pymysql.converters.conversions.update(pymysql.converters.decoders)
    engine = sqlalchemy.create_engine('mysql+pymysql://root:admin2@localhost:3306/analysisschema')

    #connection = engine.connect()
    #data.to_sql('ExpiryData', con=engine)
    data.to_sql(name='nfonemindata', con=engine, if_exists = 'append', index=False)
    #metadata = sqlalchemy.MetaData()
    #expiryData = sqlalchemy.Table('ExpiryData', metadata, autoload=True, autoload_with=engine)

    #print(repr(metadata.tables['ExpiryData']))
 
    #data.to_sql(expiryData, connection)
    
    


if __name__=="__main__":
    renameFolders()
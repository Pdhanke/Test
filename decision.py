# -*- coding: utf-8 -*-
"""
Created on Fri Jan 24 14:11:41 2020

@author: Prashant
"""

import pandas as pd
import numpy as np
from sklearn.model_selection import train_test_split
from sklearn.tree import DecisionTreeClassifier
from sklearn.tree import DecisionTreeRegressor


from sklearn.metrics import classification_report,confusion_matrix
from sklearn import preprocessing
from sklearn import utils


def getdf () :  
    df = pd.read_csv('deltaavg.csv', sep= "\t")
    
   
    df['TradeTime'] = df['TradeTime'].apply(get_sec)
    #df['TradeTime'] = 0;
    df['Ave'] =  df['Average']
    df['Close1'] = 10*df['Close2']- df['Ave']
    #df['Close1'] =0
    df['Average'] = df['Average']/df['Close2']
    #df['Close2'] = 10*df['Close2']/df['Ave']
    df['Close2'] =0
    
    del df['Ave']
    
    
    column_names_to_normalize = ['Close2']
    x = df[column_names_to_normalize].values
    min_max_scaler = preprocessing.MinMaxScaler()
    x_scaled = min_max_scaler.fit_transform(x)
    df_temp = pd.DataFrame(x, columns=column_names_to_normalize, index = df.index)
    df[column_names_to_normalize] = df_temp
    #df['Average'] = 0;
    df['Volume'] = df['Volume']//1000
    #df['Volume'] = 0;
    #df['Close2'] = 0;
    df[df==np.inf]=np.nan
    df.fillna(df.mean(), inplace=True)
    print(df.sample(n = 19))
    return df

def getdg () :  
    
    
    df = pd.read_csv('dg.csv', sep= "\t")
    
   
    df['TradeTime'] = df['TradeTime'].apply(get_sec)
    #df['TradeTime'] = 0;
    df['Ave'] =  df['Average']
    df['Close1'] = 10*df['Close2']- df['Ave']
    df['Close1'] =0
    df['Average'] = df['Average']/df['Close2']
    df['Close2'] = 10*df['Close2']/df['Ave']

    
    del df['Ave']
    
    
    column_names_to_normalize = ['Close2']
    x = df[column_names_to_normalize].values
    min_max_scaler = preprocessing.MinMaxScaler()
    x_scaled = min_max_scaler.fit_transform(x)
    
    df_temp = pd.DataFrame(x, columns=column_names_to_normalize, index = df.index)
    df[column_names_to_normalize] = df_temp
    #df['Average'] = 0;
    df['Volume'] = df['Volume']//1000
    #df['Volume'] = 0;
    #df['Close2'] = 0;
    df[df==np.inf]=np.nan
    df.fillna(df.mean(), inplace=True)
    print(df.sample(n = 19))
    return df

def mainm () :     
    
    df = getdf()
    
    y = df['delta'].astype(int) 
    X = df.drop('delta',axis=1)
    

    
    
    
    
    
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.5)

    #print(X_test.head())
    
    X_test = X_test.fillna(X_test.mean())
    X_train = X_train.fillna(X_train.mean())
    
    dtree = DecisionTreeClassifier()
    
    dtree.fit(X_train,y_train)
    
    """
  
    dg = getdg()
    y_test = dg['delta'].astype(int) 
    X_test = dg.drop('delta',axis=1)
    
  
    """
    
    
    predictions = dtree.predict(X_test)
    """
    dg['predicted'] = predictions
    dg['ytest'] = y_test
    dg.to_csv( 'decist.csv', encoding='utf-8', index=False)
    """
    print(classification_report(y_test,predictions))
    



def get_sec(time_str):
    """Get Seconds from time."""
    h, m, s = time_str.split(':')
    return int(h) * 60 + int(m)  + int(s)


if __name__=="__main__":
    mainm()
    
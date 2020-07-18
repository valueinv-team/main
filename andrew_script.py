## IMPORTING MODULES
import time
import string
import datetime
import pandas as pd
import pymysql
import os
import timeit
from decimal import *
import numpy as np
import matplotlib.pyplot as plt
import requests
import json
import csv
import logging
import matplotlib.dates 
import seaborn 
from sqlalchemy import create_engine
import gspread_dataframe as gd
import gspread
import oauth2client
import df2gspread
from oauth2client.service_account import ServiceAccountCredentials


def date_to_integer(dt_time):
    return 10000*dt_time.year + 100*dt_time.month + dt_time.day
a=0
while a<=50:
  with open('simbolos.csv') as file:
      lista=csv.reader(file)
      reg=next(lista)
  start = time.time()
  print("hello")
  # Borro el archivo que une todas las acciones
  csv_file1 = open('stocks1.csv', 'w')
  csv_file1.write("timestamp,open,high,low,close,adjusted_close,volume,dividend_amount,split_coefficient,code"+"\n")
  csv_file1.close()
  print(len(reg))
  i=0
  while i<len(reg):
  #Leo la api con los precios de las acciones
    url="https://www.alphavantage.co/query?function=TIME_SERIES_DAILY_ADJUSTED&symbol={0}&apikey=85W7D6LXQI99W74H&outputsize=compact&datatype=csv".format(str(reg[i]))  
    data=requests.get(url)
    print(reg[i],i)
    time.sleep(20)
    #Creo archivo e inserto el csv que envia la api
    sep=','
    with open('stocks.csv', 'w') as f:
      #writer = csv.writer(f)
      line_count = 0
      reader = csv.reader(data.text.splitlines())
      for row in reader:
        if line_count == 0:
            line_count+=1
        else:
            f.write(row[0]+sep+row[1]+sep+row[2]+sep+row[3]+sep+row[4]+sep+row[5]+sep+row[6]+sep+row[7]+sep+row[8]+sep+str(reg[i])+"\n")
            line_count+=1
            #writer.writerow(row) # performa mucho mas rapido que escribir linea por linea.
    print('Archivo creado.')
    csv_file1 = open('stocks1.csv', 'a')
    csv_file = open('stocks.csv', 'r')
    writer = csv.writer(csv_file1)
    reader1 = csv.reader(csv_file, delimiter=',')
    for row in reader1:
        if line_count == 0:
            line_count+=1
        else:
            writer.writerow(row)
            line_count+=1
    i+=1
    csv_file1.close()
    csv_file.close()
  ## ************************************************************
  ## ************************************************************
  ## *********** INSERTAR CSV PRICES CONSOLIDADO EN MYSQL *********** ##
  ## ************************************************************
  mydb = pymysql.connect(host='34.95.239.147',db='invest',user='root',passwd='kalamuze')
  cursor = mydb.cursor()
  line_count=0
  csv_file2 = open('stocks1.csv', 'r')
  csv_data = csv.reader(csv_file2, delimiter=',')
  cursor.execute('DELETE FROM BT_DAILY_STOCK_PRICES_TEMP')
  for row in csv_data:
      if line_count == 0:
        line_count+=1
      else:
        cursor.execute('INSERT INTO BT_DAILY_STOCK_PRICES_TEMP (stock_date,open_price,high_price,low_price,close_price,adjusted_close,volume\
        ,dividend_amount,split_coefficient,stock_code) VALUES("%s","%s","%s","%s","%s","%s","%s","%s","%s","%s")',row)
        line_count+=1
  cursor.execute('call SP_BT_DAILY_STOCK_PRICES;')
  mydb.commit()
  cursor.close()
  mydb.close()
  print("Done Prices")
  ## ************************************************************
  ## CONNECTING AND CREATING DATAFRAME WITH ALL PRICES
  ## ************************************************************
  db = pymysql.connect(host='34.95.239.147',db='invest',user='root',passwd='kalamuze')
  cursor = db.cursor()
  cursor.execute("""select * from BT_DAILY_STOCK_PRICES where stock_date>=adddate(current_date,-300) and stock_code='FB';""")
  members = cursor.fetchall()
  mp=pd.DataFrame(members)
  cursor.close()
  db.close()
  matrix=mp
  matrix.columns = ["dly_price_id","stock_date","open_price","high_price","low_price","close_price","adjusted_close",
                "volume","dividend_amount","split_coefficient","stock_code","sma9","ema9","sma24","sma50","sma150",
                "sma200","aud_ins_dt","aud_upd_dt"]
  matrix.sort_values(by=['stock_code','stock_date'], inplace=True, ascending=True)
  matrix=matrix.values
  matrix=pd.DataFrame(matrix)
  matrix.columns = ["dly_price_id","stock_date","open_price","high_price","low_price","close_price","adjusted_close",
                "volume","dividend_amount","split_coefficient","stock_code","sma9","ema9","sma24","sma50","sma150",
                "sma200","aud_ins_dt","aud_upd_dt"]
  matrix.to_csv(r'matrix.csv')
  ## CREATING DAILY PERCENT CHANGES 
  temp=matrix[:]["close_price"]
  temp = pd.DataFrame(temp)
  temp_pct=temp.pct_change()
  matrix['daily_change_pct'] = temp_pct
  ## CREATING INDICATORS
  matrix['ema9'] = matrix['close_price'].ewm(span=9,min_periods=0,adjust=False,ignore_na=False).mean()
  matrix['sma24'] = matrix['close_price'].rolling(window=24).mean()
  matrix['sma9'] = matrix['close_price'].rolling(window=9).mean()
  matrix['sma50'] = matrix['close_price'].rolling(window=50).mean()
  matrix['sma150'] = matrix['close_price'].rolling(window=150).mean()
  matrix['sma200'] = matrix['close_price'].rolling(window=200).mean()
  matrix.loc[1,"close_price"]
  ## CREATING OPERATIONS MATRIX
  data = [{'dly_price_id': 0, 'stock_date': 0, 'open_price':0,'high_price': 0, 'low_price': 0, 'close_price':0,
          'adjusted_close': 0, 'volume': 0, 'dividend_amount':0,'split_coefficient': 0, 'stock_code': 0, 'sma9':0,
          'ema9': 0, 'sma24': 0, 'sma50':0,'sma150': 0, 'sma200': 0, 'aud_ins_dt':0, 'aud_upd_dt':0, 'daily_change_pct':0,
          'operation_id':0, 'operation_type':0}]
  operation = pd.DataFrame(data)
  operation = operation.iloc[0:0]
  ope=0
  i=1
  temp=matrix[:]["dly_price_id"]
  while i<len(temp):
      if matrix.loc[i,"ema9"]>matrix.loc[i,"sma24"] and matrix.loc[(i-1),"ema9"]<matrix.loc[(i-1),"sma24"]:
          matrix.loc[i,"operation_id"]=matrix.loc[i,"dly_price_id"]
          matrix.loc[i,"operation_type"]="buy"
          operation.loc[ope]=matrix.loc[i]
          i+=1
          ope+=1
      else:
          i+=1
  buy= matrix.query('operation_type=="buy"').values
  buy = pd.DataFrame(buy)
  buy.columns = ["dly_price_id","stock_date","open_price","high_price","low_price","close_price","adjusted_close",
                "volume","dividend_amount","split_coefficient","stock_code","sma9","ema9","sma24","sma50","sma150",
                "sma200","aud_ins_dt","aud_upd_dt","daily_change_pct","operation_id","operation_type"]
  buy.loc[0,"operation_status"]="active"
  sl=-0.04
  tp=0.08
  buy_active=buy.query('operation_status!="close"')
  j=buy_active.index.values.astype(int)[0]
  jfin=buy_active.tail(1).index[0]
  print(j)
  while j<len(buy_active[:]):
    code=buy.loc[j,"stock_code"]
    ma_code=matrix.query('stock_code==@code')
    id_buy=buy.loc[j,"dly_price_id"]
    i=ma_code.query('dly_price_id==@id_buy').index.values.astype(int)[0]+1
    fin=ma_code.tail(1).index[0]+1
    while i<fin:
        pct_change_ope=(ma_code.loc[i,"open_price"]-buy.loc[j,"close_price"])/buy.loc[j,"close_price"]
        pct_change_max=(ma_code.loc[i,"high_price"]-buy.loc[j,"close_price"])/buy.loc[j,"close_price"]
        pct_change_min=(ma_code.loc[i,"low_price"]-buy.loc[j,"close_price"])/buy.loc[j,"close_price"]
        if pct_change_min<sl or pct_change_max>tp or pct_change_ope>tp or pct_change_ope<sl:
            matrix.loc[i,"operation_id"]=buy_active.loc[j,"dly_price_id"]
            matrix.loc[i,"operation_type"]="sell"
            buy.loc[j,"operation_status"]="close"
            buy_active.loc[j,"operation_status"]="close"
            operation.loc[ope]=matrix.loc[i]
            if pct_change_ope>tp or pct_change_ope<sl:
                a=matrix.loc[i,"open_price"]
                operation.loc[ope,"close_price"]=a
            elif pct_change_min<sl:
                a=buy.loc[j,"close_price"]*Decimal(1+sl)
                operation.loc[ope,"close_price"]=a
            elif pct_change_max>tp:
                a=buy.loc[j,"close_price"]*Decimal(1+tp)
                operation.loc[ope,"close_price"]=a
            i=fin
            ope+=1
        else:
            i+=1
    j+=1
  operation['aud_ins_dt'] = operation['aud_ins_dt'].astype('datetime64[ns]')
  operation['aud_upd_dt'] = operation['aud_upd_dt'].astype('datetime64[ns]')
  print("finish sell cycle")
  print("Done Update Operations")
  print("listo prices")
  xtime = datetime.datetime.now()
  print("fin",xtime)
  end = time.time()
  print("Duraciion:",end - start)
  time.sleep(10)
  a+=1
print("Finalizo completamente")
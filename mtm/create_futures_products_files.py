# -*- coding: utf-8 -*-
"""
Created on Thu Sep  8 11:40:32 2022

@author: hermann.ngayap
"""

import pandas as pd
import numpy as np
import os
from datetime import datetime
import datetime as dt
import pickle
import dis_warning
import csv
from dateutil.parser import parse

pd.options.display.float_format = '{:.3f}'.format
pd.set_option('display.max_columns', 200)
pd.set_option('display.max_rows', 200)

print("The working directory was: {0}".format(os.getcwd()))
os.chdir("C:/Users/hermann.ngayap/Boralex/Marchés Energie - FR - Equipe Marchés - Gestion de portefeuille/temp/")
print("The current working directory is: {0}".format(os.getcwd()))

filename = 'month_product.pkl'
unpickleFile = open(filename, 'rb')
month_product = pickle.load(unpickleFile)

filename = 'quarter_product.pkl'
unpickleFile = open(filename, 'rb')
quarter_product = pickle.load(unpickleFile)

filename = 'cal_product.pkl'
unpickleFile = open(filename, 'rb')
cal_product = pickle.load(unpickleFile)

#EDA
#
month_product.keys()
len(month_product.keys())
#
print(quarter_product.keys())
len(quarter_product.keys())
#
cal_product.keys()
len(cal_product.keys())
cal_product['2012'][0]

#csv path
cal_path = r"C:/Users/hermann.ngayap/Boralex\Marchés Energie - FR - Equipe Marchés - Gestion de portefeuille/temp/cal_qtr_mth/cal/"
qp_path = r"C:/Users/hermann.ngayap/Boralex/Marchés Energie - FR - Equipe Marchés - Gestion de portefeuille/temp/cal_qtr_mth/quarterly/"
mth_path = r"C:/Users/hermann.ngayap/Boralex\Marchés Energie - FR - Equipe Marchés - Gestion de portefeuille/temp/cal_qtr_mth/monthly/"
results = r"C:/Users/hermann.ngayap/Boralex/Marchés Energie - FR - Equipe Marchés - Gestion de portefeuille/temp/cal_qtr_mth/results/"

#===========================================================
#================== month product ==========================
#===========================================================

for key, value in month_product.items():
    df=pd.DataFrame(month_product[key][0])
    df.reset_index(inplace=True)
    key = pd.to_datetime(key).strftime('%y-%m-%d') 
    df.to_excel(mth_path + f"{key}.xlsx", index=False)
    
input_file_path = mth_path
output_file_path = results
excel_file_list = os.listdir(input_file_path)
df = pd.DataFrame()
for files in excel_file_list:
    if files.endswith(".xlsx"):
        #create a new dataframe to read/open each Excel file from the list of files created above
        df1 = pd.read_excel(input_file_path + files)
        #append each file into the original empty dataframe
        df = df.append(df1)
        df.reset_index(inplace=True, drop=True)     
#transfer final output to an Excel (xlsx) file on the output path
df.to_excel(output_file_path +"f2bm.xlsx", index=False)

list_dfs=[]
for file in os.listdir(input_file_path):
    if file.endswith(".xlsx"): 
        df = pd.read_excel(input_file_path + file)
        list_dfs.append(df)

all_dfs = pd.concat(list_dfs)
f2bm = pd.read_excel(output_file_path + 'f2bm.xlsx')

f2bm['quarter'] = f2bm['date product'].dt.quarter
f2bm['month'] = f2bm['date product'].dt.month
  
f2bm.loc[f2bm['month'] == 1, 'product'] = 'FBM1'
f2bm.loc[f2bm['month'] == 2, 'product'] = 'FBM2'
f2bm.loc[f2bm['month'] == 3, 'product'] = 'FBM3'
f2bm.loc[f2bm['month'] == 4, 'product'] = 'FBM4'
f2bm.loc[f2bm['month'] == 5, 'product'] = 'FBM5'
f2bm.loc[f2bm['month'] == 6, 'product'] = 'FBM6'
f2bm.loc[f2bm['month'] == 7, 'product'] = 'FBM7'
f2bm.loc[f2bm['month'] == 8, 'product'] = 'FBM8'
f2bm.loc[f2bm['month'] == 9, 'product'] = 'FBM9'
f2bm.loc[f2bm['month'] == 10, 'product'] = 'FBM10'
f2bm.loc[f2bm['month'] == 11, 'product'] = 'FBM11'
f2bm.loc[f2bm['month'] == 12, 'product'] = 'FBM12'

f2bm.to_excel(output_file_path + 'f2bm_1.xlsx', index=False)

#===========================================================
#===============     quarter product    ====================
#===========================================================

for key, value in quarter_product.items():
    df=pd.DataFrame(quarter_product[key][0])
    df.reset_index(inplace=True)
    key = pd.to_datetime(key).strftime('%y-%m-%d') 
    df.to_excel(qp_path + f"{key}.xlsx", index=False)

input_file_path = qp_path
output_file_path = results
excel_file_list = os.listdir(input_file_path)
df = pd.DataFrame()
for files in excel_file_list:
    if files.endswith(".xlsx"):
     #create a new dataframe to read/open each Excel file from the list of files created above
     df1 = pd.read_excel(input_file_path + files)
     #append each file into the original empty dataframe
     df = df.append(df1)
     df.reset_index(inplace=True, drop=True)     
#transfer final output to an Excel (xlsx) file on the output path
df.to_excel(output_file_path +"f2bq.xlsx", index=False)

list_dfs=[]
for file in os.listdir(input_file_path):
    if file.endswith(".xlsx"): 
        df = pd.read_excel(input_file_path + file)
        list_dfs.append(df)

all_dfs = pd.concat(list_dfs)

f2bq = pd.read_excel(output_file_path + 'f2bq.xlsx')
f2bq['quarter'] = f2bq['date product'].dt.quarter 

f2bq.loc[f2bq['quarter'] == 1, 'product'] = 'FBQ1'
f2bq.loc[f2bq['quarter'] == 2, 'product'] = 'FBQ2'
f2bq.loc[f2bq['quarter'] == 3, 'product'] = 'FBQ3'
f2bq.loc[f2bq['quarter'] == 4, 'product'] = 'FBQ4'

f2bq.to_excel(output_file_path + 'f2bq_1.xlsx', index=False)
#===========================================================
#==================  cal product   =========================
#===========================================================

for key, value in cal_product.items():
    df=pd.DataFrame(cal_product[key][0])
    df.reset_index(inplace=True)
    key = pd.to_datetime(key).strftime('%y-%m-%d') 
    df.to_excel(cal_path + f"{key}.xlsx", index=False)   

input_file_path = cal_path 
output_file_path = results
excel_file_list = os.listdir(input_file_path)
df = pd.DataFrame()
for files in excel_file_list:
    if files.endswith(".xlsx"):
     #create a new dataframe to read/open each Excel file from the list of files created above
     df1 = pd.read_excel(input_file_path + files)
     #append each file into the original empty dataframe
     df = df.append(df1)
     df.reset_index(inplace=True, drop=True)
     #for date in df['date product']:
         #df['date product'] = df['date product'].astype(str)
         #dt = parse(date)
         #df['date product'] = dt.date()
         #df['date product'] = pd.to_datetime(df['date product'], errors='ignore')
         #df['date product'] = df['date product'].dt.year
#transfer final output to an Excel (xlsx) file on the output path
df.to_excel(output_file_path +"f2by_.xlsx", index=False)

list_dfs=[]
for file in os.listdir(input_file_path):
    if file.endswith(".xlsx"): 
        df = pd.read_excel(input_file_path + file)
        list_dfs.append(df)

all_dfs = pd.concat(list_dfs)
f2by = pd.read_excel(output_file_path + 'f2by.xlsx')
f2by['date product'] = f2by['date product'].astype(str)
f2by['product'] = 'CAL' + f2by['date product'].str[-2:]
f2by['date product'] = f2by['date product'].astype(int)
f2by.to_excel(output_file_path + 'f2by_1.xlsx', index=False)
# def change_date_df(df):
#     format_dates_df = [col for col in df.columns if 'Date' in col];
#     for date in format_dates_df:
#         df[date] = pd.to_datetime(df[date]).apply(lambda x: x.strftime('%d-%m-%y')if not pd.isnull(x) else '');
#     return df;






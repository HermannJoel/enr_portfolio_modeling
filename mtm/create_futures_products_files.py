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
import csv
from dateutil.parser import parse
from .functions import*

month_product = load_pickle(filename='month_product.pkl')
quarter_product = load_pickle(filename='quarter_product.pkl')
cal_product = load_pickle(filename='cal_product.pkl')

def create_futures_product_files(month_product, input_file_path, output_file_path, quarter_product):
    #month product
    for key, value in month_product.items():
        df=pd.DataFrame(month_product[key][0])
        df.reset_index(inplace=True)
        key = pd.to_datetime(key).strftime('%y-%m-%d')
        load_as_excel(dest_dir=, src_flow=df, file_name=f'{key}', file_extension='.xlsx')

    input_file_path = mth_path
    output_file_path = results
    excel_file_list = os.listdir(input_file_path)
    df = pd.DataFrame()
    for files in excel_file_list:
        if files.endswith(".xlsx"):
            #create a new dataframe to read/open each Excel file from the list of files created above
            df1 = pd.read_excel(input_file_path + files)
            #append each file into the original empty dataframe
            f2bm = f2bm.append(df1)
            f2bm.reset_index(inplace=True, drop=True)     
            #transfer final output to an Excel (xlsx) file on the output path
            load_as_excel_file(dest_dir=, src_flow=f2bm, file_name='f2bm', file_extension='.xlsx')

    list_dfs=[]
    for file in os.listdir(input_file_path):
        if file.endswith(".xlsx"): 
            df = pd.read_excel(input_file_path + file)
            list_dfs.append(df)
            all_dfs = pd.concat(list_dfs)
            
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
    load_as_excel_file(dest_dir=, src_flow=f2bm, file_name='f2bm_1', file_extension='.xlsx')
    #quarter product
    for key, value in quarter_product.items():
        df=pd.DataFrame(quarter_product[key][0])
        df.reset_index(inplace=True)
        key = pd.to_datetime(key).strftime('%y-%m-%d') 
        df.to_excel(qp_path + f"{key}.xlsx", index=False)
        load_as_excel(dest_dir=, src_flow=df, file_name=f'{key}', file_extension='.xlsx')

    input_file_path = qp_path
    output_file_path = results
    excel_file_list = os.listdir(input_file_path)
    df = pd.DataFrame()
    for files in excel_file_list:
        if files.endswith(".xlsx"):
            #create a new dataframe to read/open each Excel file from the list of files created above
            df1_ = pd.read_excel(input_file_path + files)
            #append each file into the original empty dataframe
            f2bq = f2bq.append(df1_)
            f2bq.reset_index(inplace=True, drop=True)     
            #transfer final output to an Excel (xlsx) file on the output path
            load_as_excel(dest_dir=output_file_path, src_flow=f2bq, file_name='f2bq', file_extension='.xlsx')

    list_dfs=[]
    for file in os.listdir(input_file_path):
        if file.endswith(".xlsx"): 
            df = pd.read_excel(input_file_path + file)
            list_dfs.append(df)
            all_dfs = pd.concat(list_dfs)
    f2bq['quarter'] = f2bq['date product'].dt.quarter 
    f2bq.loc[f2bq['quarter'] == 1, 'product'] = 'FBQ1'
    f2bq.loc[f2bq['quarter'] == 2, 'product'] = 'FBQ2'
    f2bq.loc[f2bq['quarter'] == 3, 'product'] = 'FBQ3'
    f2bq.loc[f2bq['quarter'] == 4, 'product'] = 'FBQ4'
    load_as_excel(dest_dir=output_file_path, src_flow=f2bq, file_name='f2bq_1', file_extension='.xlsx')
    
    #cal product
    for key, value in cal_product.items():
        df=pd.DataFrame(cal_product[key][0])
        df.reset_index(inplace=True)
        key = pd.to_datetime(key).strftime('%y-%m-%d') 
        load_as_excel(dest_dir=cal_path, src_flow=df, file_name=f'{key}', file_extension='.xlsx')

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






# -*- coding: utf-8 -*-
"""
Created on Thu Sep  8 11:19:45 2022

@author: hermann.ngayap
"""
import pandas as pd
import numpy as np
import os
from datetime import datetime
import datetime as dt
import statistics

pd.options.display.float_format = '{:.3f}'.format
pd.set_option('display.max_columns', 200)
pd.set_option('display.max_rows', 200)

print("The working directory was: {0}".format(os.getcwd()))
os.chdir("C:/Users/hermann.ngayap/Boralex/Marchés Energie - FR - Equipe Marchés - Gestion de portefeuille/temp/")
print("The current working directory is: {0}".format(os.getcwd()))
#================================================
#To obtain monthly weights m1q1, m2q1, m3q1;
#m1q2, m2q2, m3q2; m1q3, m2q3, m3q3; m1q4, m2q4, m3q4   
#from quarterly products
#================================================

path_dir_in='C:/Users/hermann.ngayap/Boralex/Marchés Energie - FR - Equipe Marchés - Gestion de portefeuille/in/'
path_dir_temp='C:/Users/hermann.ngayap/Boralex/Marchés Energie - FR - Equipe Marchés - Gestion de portefeuille/temp/'

cal = pd.read_excel(path_dir_temp + 'cal_quarter_month.xlsx', sheet_name='cal')
fbqx = pd.read_excel(path_dir_temp + 'cal_quarter_month.xlsx', sheet_name='f2bq')
fbmx = pd.read_excel(path_dir_temp + 'cal_quarter_month.xlsx', sheet_name='f2bm')

dfq=fbqx.copy() 
dfm=fbmx.copy()

def months_prices(date, date_q, quarter, data_q, data_m, current_price):
    m1,m2,m3 = datetime(date_q.year,(quarter-1)*3+1,1), datetime(date_q.year,(quarter-1)*3+2,1), datetime(date_q.year,(quarter-1)*3+3,1)
    subset_df = data_m[data_m["EEX French-Baseload-Month-Future"] == date]
    subset_df = subset_df[(subset_df["date product"] == m1)|(subset_df["date product"] == m2)|(subset_df["date product"] == m3)]
    
    try:
        price1,price2,price3=float(subset_df[subset_df["date product"] == m1]["price"].values[0]), float(subset_df[subset_df["date product"] == m2]["price"].values[0]), float(subset_df[subset_df["date product"] == m3]["price"].values[0])
        liste_1=[(float(subset_df[subset_df["date product"] == m]["price"].values[0])/current_price)*100 for m in [m1,m2,m3]]
        return [date, date_q, current_price, price1,price2,price3]+liste_1
    except:
        price1,price2,price3=0,0,0
        return [date, date_q, current_price, price1,price2,price3]+[0]*3

liste_test=[]
for i in range(len(dfq)):
    if i % 1000 == 0:
        print(i)
    liste_test.append(months_prices(dfq['EEX French-Baseload-Quarter-Future'].iloc[i], dfq['date product'].iloc[i], dfq['quarter'].iloc[i], dfq, dfm, dfq["price"].iloc[i]))

data_new=pd.DataFrame(liste_test,columns=["date", "product", "price_q", "price_m1", "price_m2", "price_m3", "weight_m1", "weight_m2", "weight_m3"])
data_new["month_product"]=[data_new["product"].iloc[i].month for i in range(len(data_new))]
data_new.to_excel(path_dir_temp + "df_weights_price_q_m_.xlsx", index=False)
i+=1

data_test=data_new.loc[data_new['weight_m1'] != 0]
print(data_test.groupby(["month_product"]).mean())

df_weight = data_test.groupby( ['month_product'] ).mean().reset_index()
df_weight = df_weight[['month_product', 'weight_m1', 'weight_m2', 'weight_m3']]
df_weight.rename(columns={'month_product':'quarters'}, inplace=True)
#df_weight.to_excel(path_dir_in+'m_weights.xlsx', index=False)

#========================================================
#======================  TEST  ==========================
#========================================================

data_test['product'].dt.month.value_counts()

data_q1=data_test[data_test["product"].dt.month==1]
data_q2=data_test[data_test["product"].dt.month==4]
data_q3=data_test[data_test["product"].dt.month==7]
data_q4=data_test[data_test["product"].dt.month==10]


data_90_q1=data_q1.sample(frac=0.90)
data_10_q1=data_q1.drop(data_90_q1.index)


data_90_q1.to_excel(path_dir_temp + "data_90_q1.xlsx", index=False)
data_10_q1.to_excel(path_dir_temp + "data_10_q1.xlsx", index=False)

data_90_q2=data_q2.sample(frac=0.90)
data_10_q2=data_q2.drop(data_90_q2.index)

data_90_q2.to_excel(path_dir_temp + "data_90_q2.xlsx", index=False)
data_10_q2.to_excel(path_dir_temp + "data_10_q2.xlsx", index=False)

data_90_q3=data_q3.sample(frac=0.90)
data_10_q3=data_q3.drop(data_90_q3.index)

data_90_q3.to_excel(path_dir_temp + "data_90_q3.xlsx", index=False)
data_10_q3.to_excel(path_dir_temp + "data_10_q3.xlsx", index=False)

data_90_q4=data_q4.sample(frac=0.90)
data_10_q4=data_q4.drop(data_90_q4.index)

data_90_q4.to_excel(path_dir_temp + "data_90_q4.xlsx", index=False)
data_10_q4.to_excel(path_dir_temp + "data_10_q4.xlsx", index=False)

print(data_90_q1.groupby(["month_product"]).mean())
print(data_10_q1.groupby(["month_product"]).mean())

#==================================================================
#===To obtain quarterly weights q1, q2, q3, q4 from cal products===
#==================================================================

dfq=fbqx.copy()
dfcal=cal.copy()
#List containing  
l_q1,l_q2,l_q3,l_q4 = [],[],[],[]
i = 0
for idx, row in dfcal.iterrows():
    if i % 1000 == 0:
        print(i)
    current_price = row["price"]
    cal_date = row["EEX French-Baseload-Year-Future"]
    year = row["date product"]
    q1,q2,q3,q4 = 1, 2, 3, 4    
    subset_df = dfq[dfq["EEX French-Baseload-Quarter-Future"] == cal_date]
    subset_df = subset_df[(subset_df["date product"].dt.year == year)]
    try:
        l_q1.append((float(subset_df[subset_df["quarter"] == q1]["price"])/current_price)*100)
    except:
        pass
    try:
        l_q2.append((float(subset_df[subset_df["quarter"] == q2]["price"])/current_price)*100)
    except:
        pass
    try:
        l_q3.append((float(subset_df[subset_df["quarter"] == q3]["price"])/current_price)*100)
    except:
        pass
    try:
        l_q4.append((float(subset_df[subset_df["quarter"] == q4]["price"])/current_price)*100)
    except:
        pass        
    i+=1   
print("weight q1 :",statistics.mean(l_q1))
print("weight q2 :",statistics.mean(l_q2))
print("weight q3 :",statistics.mean(l_q3))
print("weight q4 :",statistics.mean(l_q4))

weight_1=statistics.mean(l_q1)
weight_2=statistics.mean(l_q2)
weight_3=statistics.mean(l_q3)
weight_4=statistics.mean(l_q4)

data={'quarters':['Q1', 'Q2', 'Q3', 'Q4'],
      'weights':[weight_1, weight_2, weight_3, weight_4]}
df_weight=pd.DataFrame(data=data)
df_weight.to_excel(path_dir_in+'q_weights.xlsx', index=False)

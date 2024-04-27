from datetime import datetime, timedelta 
import time
import pandas as pd
import numpy as np
from datetime import datetime as dt
import os
import sys
sys.path.append('/mnt/d/local-repo-github/enr_portfolio_modeling/')
os.chdir('/mnt/d/local-repo-github/enr_portfolio_modeling/')
from src.utils.functions import*

nb_years=(2029-dt.today().year)#2008 represents end year of time horizon. Change the year to the year that suits the desired horizon
nb_months=12#Number of months in one year
nb_quarters=nb_years*4#To compute the number of quarter in our time horizon
nb_eex_qb_cotation=5 #Nber of quarterly product cotation available in eex website.
horizon_m=(nb_months*nb_years)-2#To remove the month of July/Aug. We are now in Sep. To determine the number of month in the time horizon
horizon_q=nb_quarters-nb_eex_qb_cotation#To determine the number of quarter in the time horizon for which we have to compute prices
       
def settlement_prices_curve_estimation(yb, qb, mb, q_weights, m_weights, horizon_q = horizon_q, horizon_m = horizon_m):
    try:
        #To transfrorm cal products
        #yb['years']=["20"+yb['Delivery Period'][i][-2:] for i in range(len(yb))]
        yb['years'] = ["20" + val[-2:] if isinstance(val, str) else np.nan for val in yb['Delivery Period']]
        yb['years']=pd.to_numeric(yb['years'], errors='coerce')
        yb['Settlement Price']=pd.to_numeric(yb['Settlement Price'], errors='coerce')

        #To transform quarterly products
        #qb['Period']=["20"+qb['Delivery Period'][i][-2:]+"-0"+ qb['Delivery Period'][i][0]+"-01" for i in range(len(qb))]
        qb['Period'] = ["20" + val[-2:] + "-0" + val[0] + "-01" if isinstance(val, str) else np.nan for val in qb['Delivery Period']]
        #qb['years']=[qb['Period'][i][:4] for i in range(len(qb))]
        qb['years'] = [val[:4] if isinstance(val, str) else np.nan for val in qb['Period']]
        qb['years']=pd.to_numeric(qb['years'], errors='coerce')
    
        #qb['quarters']=[qb['Period'][i][6] for i in range(len(qb))]
        #qb['quarters'] = [val[:6] if isinstance(val, str) else np.nan for val in qb['Period']]
        qb['quarters']=[val[0] if isinstance(val, str) else np.nan for val in qb['Delivery Period']]
        qb['quarters']=pd.to_numeric(qb['quarters'], errors='coerce')
        qb['Period']=pd.to_datetime(qb['Period'])

        #To create a data frame containing all delivery period of quarterly products of time horizon
        qb_=qb.iloc[6:,:]
        list_q=[]
        date_start=qb['Period'][5]
        for i in range(horizon_q): 
            date_start += pd.DateOffset(months=3) 
            list_q.append(date_start)

        #Use cal prices and weights to compute quarterly prices
        qb_temp = pd.DataFrame({'Period': list_q})
        qb_temp['Date']='NaN'
        qb_temp['Delivery Period']='NaN'
        qb_temp['Settlement Price'] = "NaN" 
        qb_temp=qb_temp[['Date', 'Delivery Period', 'Settlement Price', 'Period']]
        qb_temp['years']=qb_temp['Period'].dt.year
        qb_temp['quarters']=qb_temp['Period'].dt.quarter
        print("Compute quarterly baseload prices starts")
        price=[]#To create an empty list that will contain quarterly prices
        for idx, row in qb_temp.iterrows():
            year=row['years']
            quarter=row['quarters']
            if (year==2024) & (quarter==2):
                price.append( yb.loc[yb['years']==2024, 'Settlement Price'] * float(q_weights.loc[q_weights['quarters']==2, 'weights'].iloc[0]))        
            elif (year==2024) & (quarter==3):
                price.append( yb.loc[yb['years']==2024, 'Settlement Price'] * float(q_weights.loc[q_weights['quarters']==3, 'weights'].iloc[0]))
            elif (year==2024) & (quarter==4):
                price.append( yb.loc[yb['years']==2024, 'Settlement Price'] * float(q_weights.loc[q_weights['quarters']==4, 'weights'].iloc[0]))
            elif (year==2025) & (quarter==1):
                price.append( yb.loc[yb['years']==2025, 'Settlement Price'] * float(q_weights.loc[q_weights['quarters']==1, 'weights'].iloc[0]))
            elif (year==2025) & (quarter==2):
                price.append( yb.loc[yb['years']==2025, 'Settlement Price'] * float(q_weights.loc[q_weights['quarters']==2, 'weights'].iloc[0]))
            elif (year==2025) & (quarter==3):
                price.append( yb.loc[yb['years']==2025, 'Settlement Price'] * float(q_weights.loc[q_weights['quarters']==3, 'weights'].iloc[0]))
            elif (year==2025) & (quarter==4):
                price.append( yb.loc[yb['years']==2025, 'Settlement Price'] * float(q_weights.loc[q_weights['quarters']==4, 'weights'].iloc[0]))
            elif (year==2026) & (quarter==1):
                price.append( yb.loc[yb['years']==2026, 'Settlement Price'] * float(q_weights.loc[q_weights['quarters']==1, 'weights'].iloc[0]))
            elif (year==2026) & (quarter==2):
                price.append( yb.loc[yb['years']==2026, 'Settlement Price'] * float(q_weights.loc[q_weights['quarters']==2, 'weights'].iloc[0]))
            elif (year==2026) & (quarter==3):
                price.append( yb.loc[yb['years']==2026, 'Settlement Price'] * float(q_weights.loc[q_weights['quarters']==3, 'weights'].iloc[0]))
            elif (year==2026) & (quarter==4):
                price.append( yb.loc[yb['years']==2026, 'Settlement Price'] * float(q_weights.loc[q_weights['quarters']==4, 'weights'].iloc[0]))
            elif (year==2027) & (quarter==1):
                price.append( yb.loc[yb['years']==2027, 'Settlement Price'] * float(q_weights.loc[q_weights['quarters']==1, 'weights'].iloc[0]))
            elif (year==2027) & (quarter==2):
                price.append( yb.loc[yb['years']==2027, 'Settlement Price'] * float(q_weights.loc[q_weights['quarters']==2, 'weights'].iloc[0]))
            elif (year==2027) & (quarter==3):
                price.append( yb.loc[yb['years']==2027, 'Settlement Price'] * float(q_weights.loc[q_weights['quarters']==3, 'weights'].iloc[0]))
            elif (year==2027) & (quarter==4):
                price.append( yb.loc[yb['years']==2027, 'Settlement Price'] * float(q_weights.loc[q_weights['quarters']==4, 'weights'].iloc[0]))
            elif (year==2028) & (quarter==1):
                price.append( yb.loc[yb['years']==2028, 'Settlement Price'] * float(q_weights.loc[q_weights['quarters']==1, 'weights'].iloc[0]))
            elif (year==2028) & (quarter==2):
                price.append( yb.loc[yb['years']==2028, 'Settlement Price'] * float(q_weights.loc[q_weights['quarters']==2, 'weights'].iloc[0]))
            elif (year==2028) & (quarter==3):
                price.append( yb.loc[yb['years']==2028, 'Settlement Price'] * float(q_weights.loc[q_weights['quarters']==3, 'weights'].iloc[0]))
            elif (year==2028) & (quarter==4):
                price.append( yb.loc[yb['years']==2028, 'Settlement Price'] * float(q_weights.loc[q_weights['quarters']==4, 'weights'].iloc[0]))

        qb_temp['Settlement Price']=[price[i].values[0] for i in range(len(price))]
        frames=[qb, qb_temp]
        prices_qb=pd.concat(frames, axis=0, ignore_index=True)
        print("Compute quarterly baseload prices starts")
        #Months
        qb=prices_qb.copy()
        mb['Delivery Period']=pd.to_datetime(mb['Delivery Period'], format='%b/%y')

        #To create a data frame containing all delivery period of monthly products of the time horizon
        list_m=[]
        date_start=mb['Delivery Period'][5]
        for i in range(horizon_m): 
            date_start += pd.DateOffset(months=1)   
            list_m.append(date_start)
        mb_temp = pd.DataFrame({'Delivery Period': list_m})
        #mb_temp =mb_temp.append(mb, ignore_index=True)

        mb_temp['years']=mb_temp['Delivery Period'].dt.year
        mb_temp['quarters']=mb_temp['Delivery Period'].dt.quarter
        mb_temp['months']=mb_temp['Delivery Period'].dt.month

        mb_=mb_temp.iloc[0:6,:]
        mb_temp_=mb_temp.iloc[6:,]
        print("Compute monthly baseload prices starts")
        price=[]#To create an empty list that will contain monthly prices
        for idx, row in mb_temp_.iterrows():
            year=row['years']
            quarter=row['quarters']
            month=row['months']
            #2023
            if (year==2023) & (quarter==1) & (month==1):
                price.append(qb.loc[(qb['years']==2023) & (qb['quarters']==1), 'Settlement Price']*float(m_weights.loc[m_weights['quarters']==1, 'weight_m1'].iloc[0]))      
            elif (year==2023) & (quarter==1) & (month==2):
                price.append(qb.loc[(qb['years']==2023) & (qb['quarters']==1), 'Settlement Price']*float(m_weights.loc[m_weights['quarters']==1, 'weight_m2'].iloc[0]))
            elif (year==2023) & (quarter==1) & (month==3):
                price.append(qb.loc[(qb['years']==2023) & (qb['quarters']==1), 'Settlement Price']*float(m_weights.loc[m_weights['quarters']==1, 'weight_m3'].iloc[0]))
            elif (year==2023) & (quarter==2) & (month==4):
                price.append(qb.loc[(qb['years']==2023) & (qb['quarters']==2), 'Settlement Price']*float(m_weights.loc[m_weights['quarters']==2, 'weight_m1'].iloc[0]))
            elif (year==2023) & (quarter==2) & (month==5):
                price.append(qb.loc[(qb['years']==2023) & (qb['quarters']==2), 'Settlement Price']*float(m_weights.loc[m_weights['quarters']==2, 'weight_m2'].iloc[0]))
            elif (year==2023) & (quarter==2) & (month==6):
                price.append(qb.loc[(qb['years']==2023) & (qb['quarters']==2), 'Settlement Price']*float(m_weights.loc[m_weights['quarters']==2, 'weight_m3'].iloc[0]))
            elif (year==2023) & (quarter==3) & (month==7):
                price.append(qb.loc[(qb['years']==2023) & (qb['quarters']==3), 'Settlement Price']*float(m_weights.loc[m_weights['quarters']==3, 'weight_m1'].iloc[0]))
            elif (year==2023) & (quarter==3) & (month==8):
                price.append(qb.loc[(qb['years']==2023) & (qb['quarters']==3), 'Settlement Price']*float(m_weights.loc[m_weights['quarters']==3, 'weight_m2'].iloc[0]))
            elif (year==2023) & (quarter==3) & (month==9):
                price.append(qb.loc[(qb['years']==2023) & (qb['quarters']==3), 'Settlement Price']*float(m_weights.loc[m_weights['quarters']==3, 'weight_m3'].iloc[0]))
            elif (year==2023) & (quarter==4) & (month==10):
                price.append(qb.loc[(qb['years']==2023) & (qb['quarters']==4), 'Settlement Price']*float(m_weights.loc[m_weights['quarters']==4, 'weight_m1'].iloc[0]))
            elif (year==2023) & (quarter==4) & (month==11):
                price.append(qb.loc[(qb['years']==2023) & (qb['quarters']==4), 'Settlement Price']*float(m_weights.loc[m_weights['quarters']==4, 'weight_m2'].iloc[0]))
            elif (year==2023) & (quarter==4) & (month==12):
                price.append(qb.loc[(qb['years']==2023) & (qb['quarters']==4), 'Settlement Price']*float(m_weights.loc[m_weights['quarters']==4, 'weight_m3'].iloc[0]))
            #2024
            elif (year==2024) & (quarter==1) & (month==1):
                price.append(qb.loc[(qb['years']==2024) & (qb['quarters']==1), 'Settlement Price']*float(m_weights.loc[m_weights['quarters']==1, 'weight_m1'].iloc[0]))
            elif (year==2024) & (quarter==1) & (month==2):
                price.append(qb.loc[(qb['years']==2024) & (qb['quarters']==1), 'Settlement Price']*float(m_weights.loc[m_weights['quarters']==1, 'weight_m2'].iloc[0]))
            elif (year==2024) & (quarter==1) & (month==3):
                price.append(qb.loc[(qb['years']==2024) & (qb['quarters']==1), 'Settlement Price']*float(m_weights.loc[m_weights['quarters']==1, 'weight_m3'].iloc[0]))
            elif (year==2024) & (quarter==2) & (month==4):
                price.append(qb.loc[(qb['years']==2024) & (qb['quarters']==2), 'Settlement Price']*float(m_weights.loc[m_weights['quarters']==2, 'weight_m1'].iloc[0]))
            elif (year==2024) & (quarter==2) & (month==5):
                price.append(qb.loc[(qb['years']==2024) & (qb['quarters']==2), 'Settlement Price']*float(m_weights.loc[m_weights['quarters']==2, 'weight_m2'].iloc[0]))
            elif (year==2024) & (quarter==2) & (month==6):
                price.append(qb.loc[(qb['years']==2024) & (qb['quarters']==2), 'Settlement Price']*float(m_weights.loc[m_weights['quarters']==2, 'weight_m3'].iloc[0]))
            elif (year==2024) & (quarter==3) & (month==7):
                price.append(qb.loc[(qb['years']==2024) & (qb['quarters']==3), 'Settlement Price']*float(m_weights.loc[m_weights['quarters']==3, 'weight_m1'].iloc[0]))
            elif (year==2024) & (quarter==3) & (month==8):
                price.append(qb.loc[(qb['years']==2024) & (qb['quarters']==3), 'Settlement Price']*float(m_weights.loc[m_weights['quarters']==3, 'weight_m2'].iloc[0]))
            elif (year==2024) & (quarter==3) & (month==9):
                price.append(qb.loc[(qb['years']==2024) & (qb['quarters']==3), 'Settlement Price']*float(m_weights.loc[m_weights['quarters']==3, 'weight_m3'].iloc[0]))
            elif (year==2024) & (quarter==4) & (month==10):
                price.append(qb.loc[(qb['years']==2024) & (qb['quarters']==4), 'Settlement Price']*float(m_weights.loc[m_weights['quarters']==4, 'weight_m1'].iloc[0]))
            elif (year==2024) & (quarter==4) & (month==11):
                price.append(qb.loc[(qb['years']==2024) & (qb['quarters']==4), 'Settlement Price']*float(m_weights.loc[m_weights['quarters']==4, 'weight_m2'].iloc[0]))
            elif (year==2024) & (quarter==4) & (month==12):
                price.append(qb.loc[(qb['years']==2024) & (qb['quarters']==4), 'Settlement Price']*float(m_weights.loc[m_weights['quarters']==4, 'weight_m3'].iloc[0]))
            #2025
            elif (year==2025) & (quarter==1) & (month==1):
                price.append(qb.loc[(qb['years']==2025) & (qb['quarters']==1), 'Settlement Price']*float(m_weights.loc[m_weights['quarters']==1, 'weight_m1'].iloc[0]))
            elif (year==2025) & (quarter==1) & (month==2):
                price.append(qb.loc[(qb['years']==2025) & (qb['quarters']==1), 'Settlement Price']*float(m_weights.loc[m_weights['quarters']==1, 'weight_m2'].iloc[0]))
            elif (year==2025) & (quarter==1) & (month==3):
                price.append(qb.loc[(qb['years']==2025) & (qb['quarters']==1), 'Settlement Price']*float(m_weights.loc[m_weights['quarters']==1, 'weight_m3'].iloc[0]))
            elif (year==2025) & (quarter==2) & (month==4):
                price.append(qb.loc[(qb['years']==2025) & (qb['quarters']==2), 'Settlement Price']*float(m_weights.loc[m_weights['quarters']==2, 'weight_m1'].iloc[0]))
            elif (year==2025) & (quarter==2) & (month==5):
                price.append(qb.loc[(qb['years']==2025) & (qb['quarters']==2), 'Settlement Price']*float(m_weights.loc[m_weights['quarters']==2, 'weight_m2'].iloc[0]))
            elif (year==2025) & (quarter==2) & (month==6):
                price.append(qb.loc[(qb['years']==2025) & (qb['quarters']==2), 'Settlement Price']*float(m_weights.loc[m_weights['quarters']==2, 'weight_m3'].iloc[0]))
            elif (year==2025) & (quarter==3) & (month==7):
                price.append(qb.loc[(qb['years']==2025) & (qb['quarters']==3), 'Settlement Price']*float(m_weights.loc[m_weights['quarters']==3, 'weight_m1'].iloc[0]))
            elif (year==2025) & (quarter==3) & (month==8):
                price.append(qb.loc[(qb['years']==2025) & (qb['quarters']==3), 'Settlement Price']*float(m_weights.loc[m_weights['quarters']==3, 'weight_m2'].iloc[0]))
            elif (year==2025) & (quarter==3) & (month==9):
                price.append(qb.loc[(qb['years']==2025) & (qb['quarters']==3), 'Settlement Price']*float(m_weights.loc[m_weights['quarters']==3, 'weight_m3'].iloc[0]))
            elif (year==2025) & (quarter==4) & (month==10):
                price.append(qb.loc[(qb['years']==2025) & (qb['quarters']==4), 'Settlement Price']*float(m_weights.loc[m_weights['quarters']==4, 'weight_m1'].iloc[0]))
            elif (year==2025) & (quarter==4) & (month==11):
                price.append(qb.loc[(qb['years']==2025) & (qb['quarters']==4), 'Settlement Price']*float(m_weights.loc[m_weights['quarters']==4, 'weight_m2'].iloc[0]))
            elif (year==2025) & (quarter==4) & (month==12):
                price.append(qb.loc[(qb['years']==2025) & (qb['quarters']==4), 'Settlement Price']*float(m_weights.loc[m_weights['quarters']==4, 'weight_m3'].iloc[0]))
            #2026
            elif (year==2026) & (quarter==1) & (month==1):
                price.append(qb.loc[(qb['years']==2026) & (qb['quarters']==1), 'Settlement Price']*float(m_weights.loc[m_weights['quarters']==1, 'weight_m1'].iloc[0]))
            elif (year==2026) & (quarter==1) & (month==2):
                price.append(qb.loc[(qb['years']==2026) & (qb['quarters']==1), 'Settlement Price']*float(m_weights.loc[m_weights['quarters']==1, 'weight_m2'].iloc[0]))
            elif (year==2026) & (quarter==1) & (month==3):
                price.append(qb.loc[(qb['years']==2026) & (qb['quarters']==1), 'Settlement Price']*float(m_weights.loc[m_weights['quarters']==1, 'weight_m3'].iloc[0]))
            elif (year==2026) & (quarter==2) & (month==4):
                price.append(qb.loc[(qb['years']==2026) & (qb['quarters']==2), 'Settlement Price']*float(m_weights.loc[m_weights['quarters']==2, 'weight_m1'].iloc[0]))
            elif (year==2026) & (quarter==2) & (month==5):
                price.append(qb.loc[(qb['years']==2026) & (qb['quarters']==2), 'Settlement Price']*float(m_weights.loc[m_weights['quarters']==2, 'weight_m2'].iloc[0]))
            elif (year==2026) & (quarter==2) & (month==6):
                price.append(qb.loc[(qb['years']==2026) & (qb['quarters']==2), 'Settlement Price']*float(m_weights.loc[m_weights['quarters']==2, 'weight_m3'].iloc[0]))
            elif (year==2026) & (quarter==3) & (month==7):
                price.append(qb.loc[(qb['years']==2026) & (qb['quarters']==3), 'Settlement Price']*float(m_weights.loc[m_weights['quarters']==3, 'weight_m1'].iloc[0]))
            elif (year==2026) & (quarter==3) & (month==8):
                price.append(qb.loc[(qb['years']==2026) & (qb['quarters']==3), 'Settlement Price']*float(m_weights.loc[m_weights['quarters']==3, 'weight_m2'].iloc[0]))
            elif (year==2026) & (quarter==3) & (month==9):
                price.append(qb.loc[(qb['years']==2026) & (qb['quarters']==3), 'Settlement Price']*float(m_weights.loc[m_weights['quarters']==3, 'weight_m3'].iloc[0]))
            elif (year==2026) & (quarter==4) & (month==10):
                price.append(qb.loc[(qb['years']==2026) & (qb['quarters']==4), 'Settlement Price']*float(m_weights.loc[m_weights['quarters']==4, 'weight_m1'].iloc[0]))
            elif (year==2026) & (quarter==4) & (month==11):
                price.append(qb.loc[(qb['years']==2026) & (qb['quarters']==4), 'Settlement Price']*float(m_weights.loc[m_weights['quarters']==4, 'weight_m2'].iloc[0]))
            elif (year==2026) & (quarter==4) & (month==12):
                price.append(qb.loc[(qb['years']==2026) & (qb['quarters']==4), 'Settlement Price']*float(m_weights.loc[m_weights['quarters']==4, 'weight_m3'].iloc[0]))
            #2027
            elif (year==2027) & (quarter==1) & (month==1):
                price.append(qb.loc[(qb['years']==2027) & (qb['quarters']==1), 'Settlement Price']*float(m_weights.loc[m_weights['quarters']==1, 'weight_m1'].iloc[0]))
            elif (year==2027) & (quarter==1) & (month==2):
                price.append(qb.loc[(qb['years']==2027) & (qb['quarters']==1), 'Settlement Price']*float(m_weights.loc[m_weights['quarters']==1, 'weight_m2'].iloc[0]))
            elif (year==2027) & (quarter==1) & (month==3):
                price.append(qb.loc[(qb['years']==2027) & (qb['quarters']==1), 'Settlement Price']*float(m_weights.loc[m_weights['quarters']==1, 'weight_m3'].iloc[0]))
            elif (year==2027) & (quarter==2) & (month==4):
                price.append(qb.loc[(qb['years']==2027) & (qb['quarters']==2), 'Settlement Price']*float(m_weights.loc[m_weights['quarters']==2, 'weight_m1'].iloc[0]))
            elif (year==2027) & (quarter==2) & (month==5):
                price.append(qb.loc[(qb['years']==2027) & (qb['quarters']==2), 'Settlement Price']*float(m_weights.loc[m_weights['quarters']==2, 'weight_m2'].iloc[0]))
            elif (year==2027) & (quarter==2) & (month==6):
                price.append(qb.loc[(qb['years']==2027) & (qb['quarters']==2), 'Settlement Price']*float(m_weights.loc[m_weights['quarters']==2, 'weight_m3'].iloc[0]))
            elif (year==2027) & (quarter==3) & (month==7):
                price.append(qb.loc[(qb['years']==2027) & (qb['quarters']==3), 'Settlement Price']*float(m_weights.loc[m_weights['quarters']==3, 'weight_m1'].iloc[0]))
            elif (year==2027) & (quarter==3) & (month==8):
                price.append(qb.loc[(qb['years']==2027) & (qb['quarters']==3), 'Settlement Price']*float(m_weights.loc[m_weights['quarters']==3, 'weight_m2'].iloc[0]))
            elif (year==2027) & (quarter==3) & (month==9):
                price.append(qb.loc[(qb['years']==2027) & (qb['quarters']==3), 'Settlement Price']*float(m_weights.loc[m_weights['quarters']==3, 'weight_m3'].iloc[0]))
            elif (year==2027) & (quarter==4) & (month==10):
                price.append(qb.loc[(qb['years']==2027) & (qb['quarters']==4), 'Settlement Price']*float(m_weights.loc[m_weights['quarters']==4, 'weight_m1'].iloc[0]))
            elif (year==2027) & (quarter==4) & (month==11):
                price.append(qb.loc[(qb['years']==2027) & (qb['quarters']==4), 'Settlement Price']*float(m_weights.loc[m_weights['quarters']==4, 'weight_m2'].iloc[0]))
            elif (year==2027) & (quarter==4) & (month==12):
                price.append(qb.loc[(qb['years']==2027) & (qb['quarters']==4), 'Settlement Price']*float(m_weights.loc[m_weights['quarters']==4, 'weight_m3'].iloc[0]))
            #2028
            elif (year==2028) & (quarter==1) & (month==1):
                price.append(qb.loc[(qb['years']==2028) & (qb['quarters']==1), 'Settlement Price']*float(m_weights.loc[m_weights['quarters']==1, 'weight_m1'].iloc[0]))
            elif (year==2028) & (quarter==1) & (month==2):
                price.append(qb.loc[(qb['years']==2028) & (qb['quarters']==1), 'Settlement Price']*float(m_weights.loc[m_weights['quarters']==1, 'weight_m2'].iloc[0]))
            elif (year==2028) & (quarter==1) & (month==3):
                price.append(qb.loc[(qb['years']==2028) & (qb['quarters']==1), 'Settlement Price']*float(m_weights.loc[m_weights['quarters']==1, 'weight_m3'].iloc[0]))
            elif (year==2028) & (quarter==2) & (month==4):
                price.append(qb.loc[(qb['years']==2028) & (qb['quarters']==2), 'Settlement Price']*float(m_weights.loc[m_weights['quarters']==2, 'weight_m1'].iloc[0]))
            elif (year==2028) & (quarter==2) & (month==5):
                price.append(qb.loc[(qb['years']==2028) & (qb['quarters']==2), 'Settlement Price']*float(m_weights.loc[m_weights['quarters']==2, 'weight_m2'].iloc[0]))
            elif (year==2028) & (quarter==2) & (month==6):
                price.append(qb.loc[(qb['years']==2028) & (qb['quarters']==2), 'Settlement Price']*float(m_weights.loc[m_weights['quarters']==2, 'weight_m3'].iloc[0]))
            elif (year==2028) & (quarter==3) & (month==7):
                price.append(qb.loc[(qb['years']==2028) & (qb['quarters']==3), 'Settlement Price']*float(m_weights.loc[m_weights['quarters']==3, 'weight_m1'].iloc[0]))
            elif (year==2028) & (quarter==3) & (month==8):
                price.append(qb.loc[(qb['years']==2028) & (qb['quarters']==3), 'Settlement Price']*float(m_weights.loc[m_weights['quarters']==3, 'weight_m2'].iloc[0]))
            elif (year==2028) & (quarter==3) & (month==9):
                price.append(qb.loc[(qb['years']==2028) & (qb['quarters']==3), 'Settlement Price']*float(m_weights.loc[m_weights['quarters']==3, 'weight_m3'].iloc[0]))
            elif (year==2028) & (quarter==4) & (month==10):
                price.append(qb.loc[(qb['years']==2028) & (qb['quarters']==4), 'Settlement Price']*float(m_weights.loc[m_weights['quarters']==4, 'weight_m1'].iloc[0]))
            elif (year==2028) & (quarter==4) & (month==11):
                price.append(qb.loc[(qb['years']==2028) & (qb['quarters']==4), 'Settlement Price']*float(m_weights.loc[m_weights['quarters']==4, 'weight_m2'].iloc[0]))
            elif (year==2028) & (quarter==4) & (month==12):
                price.append(qb.loc[(qb['years']==2028) & (qb['quarters']==4), 'Settlement Price']*float(m_weights.loc[m_weights['quarters']==4, 'weight_m3'].iloc[0]))
        print("Compute monthly baseload prices ends")
        mb_temp_['Settlement Price']=[price[i].values[0] for i in range(len(price))]
        frames=[mb_, mb_temp_]
        prices_mb=pd.concat(frames, axis=0, ignore_index=True)
        #prices_mb['cotation_date']=prices_mb['Date'][0]
        prices_mb['CotationDate']=mb['Date'][0]
        prices_mb=prices_mb[['Delivery Period', 'Settlement Price', 'CotationDate']]
        prices_mb.rename(columns = {"Delivery Period":"DeliveryPeriod", "Settlement Price":"SettlementPrice"
                     }, inplace = True)
    
        return prices_mb
    except Exception as e:
        print("Compute settlement prices error!: "+str(e))

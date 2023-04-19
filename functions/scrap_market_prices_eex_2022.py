from datetime import datetime 
import requests
import pickle
import time
from webdriver_manager.chrome import ChromeDriverManager
import warnings
warnings.filterwarnings("ignore")
import pandas as pd
from datetime import  timedelta
import numpy as np
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.keys import Keys
from datetime import datetime as dt
import os

path=                   #cookie file path
year=2022
filename=f"Futures_products_{dt.today().year}.xlsx"


def change(x):
    if x=='-':
        return x
    else:
        x=str(x)
        try:
            
            x=x.replace('\u202f','')
        except:
            x=x.replace('.', '')
        x=x.replace(',', '.')
        x=float(x)
        return x 
    
def load_cookie(driver, path):
    with open(path, 'rb') as cookiesfile:
        cookies = pickle.load(cookiesfile)
        for cookie in cookies:
            driver.add_cookie(cookie)
                         
def create_excel(dates,url,date1):
    chrome_options = Options()
    chrome_options.add_argument('--headless')
    chrome_options.add_argument('--no-sandbox')
    chrome_options.add_argument('--disable-dev-shm-usage')
    chrome_options.add_argument("--disable-extensions")
    driver = webdriver.Chrome(ChromeDriverManager().install(),chrome_options=chrome_options)
    driver.maximize_window()
    driver.get(url)
    load_cookie(driver,'cookies.pkl' )
    i=0
    time.sleep(5)
    S=[[np.nan]*3 for i in range(6*len(dates))]
    date1=dates[0]
    
    
    #.find_elements_by_tag_name('input')[32].click()
    driver.find_elements_by_class_name("filter-option-inner-inner")
    time.sleep(5)

    select=driver.find_elements_by_xpath('//select[@class="selectpicker"]/option')
    data_date=2
    for k,c in enumerate(select) :
        if c.get_attribute('value')=='EEX French Power Futures':
            data_date+=k
            c.click()
    time.sleep(2)
    
    ####################configure date  
    
    
    element = driver.find_element_by_xpath('//*[@id="symbolheader_pffr"]/div/div[1]/div/input')
    element.clear()
    element.send_keys(date1,Keys.ENTER)
    
    time.sleep(2)
                   
    #####################################    
    ###Month
    print('here we go:\n')
    print(date1+'\n')
    
    button = driver.find_element_by_xpath('//*[@id="symbolheader_pffr"]/div/div[2]/div[4]')
    driver.execute_script("arguments[0].click();", button)   
    time.sleep(6)
    MB=[]
    MP=[]
    table_id = driver.find_elements(By.CLASS_NAME, 'mv-quote')
    table_MB=table_id[8]
    rows = table_MB.find_elements(By.TAG_NAME, "tr") # get all of the rows in the table
    columns=rows[0].find_elements(By.TAG_NAME, "th")
    columns=[s.text for s in columns[:-1]]
    for row in rows[2:]:
        # Get the columns (all the column 2)
        col = row.find_elements(By.TAG_NAME, "td")#note: index start from 0, 1 is col 2
        col=[s.text for s in col]
        MB.append(col[:-1])
        
        
    df_MB=pd.DataFrame(MB,columns=columns)
    DFMB=pd.DataFrame(S,columns=['Date','Delivery Period','Settlement Price'])
    DFMB['Date']=date1
    DFMB['Delivery Period']=df_MB['Name'][:6]
    DFMB['Settlement Price']=df_MB['Settlement Price'][:6]
    DFMB['Settlement Price']=DFMB['Settlement Price'].apply(lambda x:change(x))
    print("Monthly data:\n")
    print("Baseload:\n")
    print(DFMB)
    if all(DFMB['Settlement Price']==['-','-','-','-','-','-']):
        driver.quit()
        del driver
        raise NameError('no data')
    
    
    table_MP=table_id[9]
    rows = table_MP.find_elements(By.TAG_NAME, "tr") # get all of the rows in the table
    columns=rows[0].find_elements(By.TAG_NAME, "th")
    columns=[s.text for s in columns[:-1]]
    for row in rows[2:]:
        # Get the columns (all the column 2)
        col = row.find_elements(By.TAG_NAME, "td")#note: index start from 0, 1 is col 2
        col=[s.text for s in col]
        MP.append(col[:-1])
       
    df_MP=pd.DataFrame(MP,columns=columns)
    DFMP=pd.DataFrame(S,columns=['Date','Delivery Period','Settlement Price'])
    DFMP['Date']=date1
    DFMP['Delivery Period']=df_MP['Name'][:6]
    DFMP['Settlement Price']=df_MP['Settlement Price'][:6]
    DFMP['Settlement Price']=DFMP['Settlement Price'].apply(lambda x:change(x))
    print("\nPeakload:\n")
    print(DFMP)

    
    ###Quarter
    button = driver.find_element_by_xpath('//*[@id="symbolheader_pffr"]/div/div[2]/div[5]')
    driver.execute_script("arguments[0].click();", button)   
    QB=[]
    QP=[]
    time.sleep(5)
    table_id = driver.find_elements(By.CLASS_NAME, 'mv-quote')

    table_QB=table_id[8]
    rows = table_QB.find_elements(By.TAG_NAME, "tr") # get all of the rows in the table
    columns=rows[0].find_elements(By.TAG_NAME, "th")
    columns=[s.text for s in columns[:-1]]
    for row in rows[2:]:
        # Get the columns (all the column 2)
        col = row.find_elements(By.TAG_NAME, "td")#note: index start from 0, 1 is col 2
        col=[s.text for s in col]
        QB.append(col[:-1])
        
    df_QB=pd.DataFrame(QB,columns=columns)
    DFQB=pd.DataFrame(S,columns=['Date','Delivery Period','Settlement Price'])
    DFQB['Date']=date1
    DFQB['Delivery Period']=df_QB['Name'][:6]
    DFQB['Settlement Price']=df_QB['Settlement Price'][:6]
    DFQB['Settlement Price']=DFQB['Settlement Price'].apply(lambda x:change(x))
    print('\nQuarterly data:\n')
    print("Baseload:\n")
    print(DFQB)
    fbqx=DFQB.copy()

    
    table_QP=table_id[9]
    rows = table_QP.find_elements(By.TAG_NAME, "tr") # get all of the rows in the table
    columns=rows[0].find_elements(By.TAG_NAME, "th")
    columns=[s.text for s in columns[:-1]]
    for row in rows[2:]:
        # Get the columns (all the column 2)
        col = row.find_elements(By.TAG_NAME, "td")#note: index start from 0, 1 is col 2
        col=[s.text for s in col]
        QP.append(col[:-1])
        
    df_QP=pd.DataFrame(QP,columns=columns)
    DFQP=pd.DataFrame(S,columns=['Date','Delivery Period','Settlement Price'])
    DFQP['Date']=date1
    DFQP['Delivery Period']=df_QP['Name'][:6]
    DFQP['Settlement Price']=df_QP['Settlement Price'][:6]
    DFQP['Settlement Price']=DFQP['Settlement Price'].apply(lambda x:change(x))
    print("\nPeakload:\n")
    print(DFQP)
    
    
    ####Year
    button = driver.find_element_by_xpath('//*[@id="symbolheader_pffr"]/div/div[2]/div[6]')
    driver.execute_script("arguments[0].click();", button)   
    YB=[]
    YP=[]
    time.sleep(10)
    table_id = driver.find_elements(By.CLASS_NAME, 'mv-quote')
    table_YB=table_id[8]
    rows = table_YB.find_elements(By.TAG_NAME, "tr") # get all of the rows in the table
    columns=rows[0].find_elements(By.TAG_NAME, "th")
    columns=[s.text for s in columns[:-1]]
    for row in rows[2:]:
        # Get the columns (all the column 2)
        col = row.find_elements(By.TAG_NAME, "td")#note: index start from 0, 1 is col 2
        col=[s.text for s in col]
        YB.append(col[:-1])
        
    df_YB=pd.DataFrame(YB,columns=columns)
    DFYB=pd.DataFrame(S,columns=['Date','Delivery Period','Settlement Price'])
    DFYB['Date']=date1
    DFYB['Delivery Period']=df_YB['Name'][:6]
    DFYB['Settlement Price']=df_YB['Settlement Price'][:6]
    DFYB['Settlement Price']=DFYB['Settlement Price'].apply(lambda x:change(x))
    print("\nYearly data:\n")
    print("Baseload:\n")    
    print(DFYB)
    cal=DFYB.copy()
    
    table_YP=table_id[9]
    rows = table_YP.find_elements(By.TAG_NAME, "tr") # get all of the rows in the table
    columns=rows[0].find_elements(By.TAG_NAME, "th")
    columns=[s.text for s in columns[:-1]]
    for row in rows[2:]:
        # Get the columns (all the column 2)
        col = row.find_elements(By.TAG_NAME, "td")#note: index start from 0, 1 is col 2
        col=[s.text for s in col]
        YP.append(col[:-1])
        
    df_YP=pd.DataFrame(YP,columns=columns)
    DFYP=pd.DataFrame(S,columns=['Date','Delivery Period','Settlement Price'])

    DFYP['Date']=date1
    DFYP['Delivery Period']=df_YP['Name'][:6]
    DFYP['Settlement Price']=df_YP['Settlement Price'][:6]
    DFYP['Settlement Price']=DFYP['Settlement Price'].apply(lambda x:change(x))
    print("\nPeakload:\n")
    print(DFYP)

    name_excel2=filename
    df_BME=pd.read_excel(name_excel2,sheet_name="MB")
    #alert_eex(df_BME,DFMB,"MB")
    df_PME=pd.read_excel(name_excel2,sheet_name="MP")
    #alert_eex(df_PME,DFMP,"MP")        
    df_BQE=pd.read_excel(name_excel2,sheet_name="QB")
    #alert_eex(df_BQE,DFQB,"QB")
    df_PQE=pd.read_excel(name_excel2,sheet_name="QP")
    #alert_eex(df_PQE,DFQP,"QP")        
    df_BYE=pd.read_excel(name_excel2,sheet_name="YB")
    #alert_eex(df_BYE,DFYB,"YB")
    df_PYE=pd.read_excel(name_excel2,sheet_name="YP")
    #alert_eex(df_PYE,DFYP,"YP")

    DFMB=pd.concat([DFMB,df_BME])
    DFMP=pd.concat([DFMP,df_PME])
    DFQB=pd.concat([DFQB,df_BQE])
    DFQP=pd.concat([DFQP,df_PQE])
    DFYB=pd.concat([DFYB,df_BYE])
    DFYP=pd.concat([DFYP,df_PYE])
    with pd.ExcelWriter(filename) as writer:  # doctest: +SKIP
        DFMB.to_excel(writer, sheet_name='MB',index=False)
        DFMP.to_excel(writer, sheet_name='MP',index=False)
        DFQB.to_excel(writer, sheet_name='QB',index=False)
        DFQP.to_excel(writer, sheet_name='QP',index=False)
        DFYB.to_excel(writer, sheet_name='YB',index=False)
        DFYP.to_excel(writer, sheet_name='YP',index=False)
    
    driver.quit()
    del driver

def scrap_eex(i):
    url='https://www.eex.com/en/market-data/power/futures'
    Datescrap=str(dt.today()-timedelta(days=i)).split(' ')[0]
    if (dt.today()-timedelta(days=i)).weekday() in [5,6]:
        name_excel2=filename
        df_BME=pd.read_excel(name_excel2,sheet_name="MB")
        df_PME=pd.read_excel(name_excel2,sheet_name="MP")        
        df_BQE=pd.read_excel(name_excel2,sheet_name="QB")
        df_PQE=pd.read_excel(name_excel2,sheet_name="QP")        
        df_BYE=pd.read_excel(name_excel2,sheet_name="YB")
        df_PYE=pd.read_excel(name_excel2,sheet_name="YP")
        df_BME2=df_BME[:6].copy()
        df_BME2['Date']=Datescrap
        df_PME2=df_PME[:6].copy()
        df_PME2['Date']=Datescrap
        df_BQE2=df_BQE[:6].copy()
        df_BQE2['Date']=Datescrap
        df_PQE2=df_PQE[:6].copy()
        df_PQE2['Date']=Datescrap
        df_BYE2=df_BYE[:6].copy()
        df_BYE2['Date']=Datescrap
        df_PYE2=df_PYE[:6].copy()
        df_PYE2['Date']=Datescrap

        DFMB=pd.concat([df_BME2,df_BME])
        DFMP=pd.concat([df_PME2,df_PME])
        DFQB=pd.concat([df_BQE2,df_BQE])
        DFQP=pd.concat([df_PQE2,df_PQE])
        DFYB=pd.concat([df_BYE2,df_BYE])
        DFYP=pd.concat([df_PYE2,df_PYE])
        
        
        with pd.ExcelWriter(filename) as writer:  # doctest: +SKIP
            DFMB.to_excel(writer, sheet_name='MB',index=False)
            DFMP.to_excel(writer, sheet_name='MP',index=False)
            DFQB.to_excel(writer, sheet_name='QB',index=False)
            DFQP.to_excel(writer, sheet_name='QP',index=False)
            DFYB.to_excel(writer, sheet_name='YB',index=False)
            DFYP.to_excel(writer, sheet_name='YP',index=False)
    else:   
        
        try:
            dates=[Datescrap]
            create_excel(dates,url,Datescrap)
        
        except Exception as e: # work on python 3.x
            print(str(e))
            #alert(sender,password,str(e))
            if dt.today()==dt(year,1,1):
                name_excel2=f"Futures_products_{dt.today().year-1}.xlsx"
            else:
                name_excel2=f"Futures_products_{dt.today().year}.xlsx"
            df_BME=pd.read_excel(name_excel2,sheet_name="MB")
            df_PME=pd.read_excel(name_excel2,sheet_name="MP")
            df_BQE=pd.read_excel(name_excel2,sheet_name="QB")
            df_PQE=pd.read_excel(name_excel2,sheet_name="QP")        
            df_BYE=pd.read_excel(name_excel2,sheet_name="YB")
            df_PYE=pd.read_excel(name_excel2,sheet_name="YP")
            df_BME2=df_BME[:6].copy()
            df_BME2['Date']=Datescrap
            df_PME2=df_PME[:6].copy()
            df_PME2['Date']=Datescrap
            df_BQE2=df_BQE[:6].copy()
            df_BQE2['Date']=Datescrap
            df_PQE2=df_PQE[:6].copy()
            df_PQE2['Date']=Datescrap
            df_BYE2=df_BYE[:6].copy()
            df_BYE2['Date']=Datescrap
            df_PYE2=df_PYE[:6].copy()
            df_PYE2['Date']=Datescrap
    
            DFMB=pd.concat([df_BME2,df_BME])
            DFMP=pd.concat([df_PME2,df_PME])
            DFQB=pd.concat([df_BQE2,df_BQE])
            DFQP=pd.concat([df_PQE2,df_PQE])
            DFYB=pd.concat([df_BYE2,df_BYE])
            DFYP=pd.concat([df_PYE2,df_PYE])
            
            
            with pd.ExcelWriter(filename) as writer:  # doctest: +SKIP
                DFMB.to_excel(writer, sheet_name='MB',index=False)
                DFMP.to_excel(writer, sheet_name='MP',index=False)
                DFQB.to_excel(writer, sheet_name='QB',index=False)
                DFQP.to_excel(writer, sheet_name='QP',index=False)
                DFYB.to_excel(writer, sheet_name='YB',index=False)
                DFYP.to_excel(writer, sheet_name='YP',index=False)

    
def do_scrap_eex(i):    
    print("futures scrapping starts")
    scrap_eex(i)
    print("futures scrapping is done")
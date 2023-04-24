import pandas as pd
import numpy as np
xrange = range
import os
import configparser
from datetime import datetime
import sys
pd.options.mode.chained_assignment = None
os.chdir('D:/local-repo-github/enr_portfolio_modeling/')
from functions import* 

#Load Config
config_file=os.path.join(os.path.dirname("__file__"), 'Config/config.ini') 
config=configparser.ConfigParser(allow_no_value=True)
config.read(config_file)

# Initialize Variables
src_dir=os.path.join(os.path.dirname("__file__"),config['develop']['src_dir'])
dest_dir=os.path.join(os.path.dirname("__file__"),config['develop']['dest_dir'])
template_asset=os.path.join(os.path.dirname("__file__"),config['develop']['template_asset'])
ppa=os.path.join(os.path.dirname("__file__"),config['develop']['ppa'])


def extract(template_asset_path, ppa_path):
    ''' Function to extract excel files.
    Parameters
    ==========
    template_asset_path: str
        path excel file containing data hedge in prod
    ppa_path: str
        path excel file containing data hedge in planif    
    Returns
    =======
    df_template_asset: DataFrame
        contracts prices asset in prod dataframe
    df_ppa: DataFrame
        
    '''
    try:
        df_template_asset=ReadExcelFile(template_asset_path)
        ppa=ReadExcelFile(ppa_path)
        
        return df_template_asset, df_ppa 
    except Exception as e:
        print("Data Extraction error!: "+str(e))

        
def transform(template_asset, df_ppa, **kwargs):
    """
    udf Function to generate template contracts prices asset in prod
    Parameters
    ===========
    **kwargs
        hedge_vmr: DataFrame
                
        hedge_planif: DataFrame
    prices: DataFrame
        data frame contract prices
    template_asset: DataFrame
    Returns
    =======
    template_prices: DataFrame
        template prices dataframe
    """
    try:
        ppa_= df_ppa
        ppa_=ppa_.iloc[:,np.r_[0, 1, 2, 4, 5, 6, -1]]
        #Import date cod & date_dementelement from asset
        asset_ = df_template_asset
        asset_=asset_[['asset_id', 'projet_id', 'cod', 'date_dementelement', 'date_merchant']]
        asset_.reset_index(drop=True, inplace=True)
        #To merge ppa prices data and template asset 
        ppa=pd.merge(ppa_, asset_, how='left', on=['projet_id'])

        #To create a df containing all the dates within the time horizon  
        df = ppa.copy()
        nbr = len(df)     
        start_date = pd.to_datetime([date_obj] * nbr)
        d = pd.DataFrame()
        for i in range(0, horizon):
            df_buffer= df 
            df_buffer["date"] = start_date
            d = pd.concat([d, df_buffer], axis=0)
            start_date= start_date + pd.DateOffset(months=1)

        #reset index    
        d.reset_index(drop=True, inplace=True)
        #To create quarter and month columns
        d['année'] = d['date'].dt.year
        d['trimestre'] = d['date'].dt.quarter
        d['mois'] = d['date'].dt.month

        #To remove price based on date_debut
        RemoveContractPrices(d, *args, **kwargs)
        #Condition:date column is less (in total seconds) than first date for each projet_id's first date_cod value
        cond=((d['date'] - d.groupby(['hedge_id', 'projet_id'])['date_debut'].transform('first')).dt.total_seconds())<0
        d['price'] = np.where(cond,'', d['price'])
        #To remove price based on date_fin
        cond_2=((d['date'] - d.groupby(['hedge_id', 'projet_id'])['date_fin'].transform('first')).dt.total_seconds())>0
        d['price'] = np.where(cond_2, '', d['price'])
        #To remove price based on date_dementelemnt
        cond_2=((d['date'] - d.groupby(['hedge_id', 'projet_id'])['date_dementelement'].transform('first')).dt.total_seconds())>0
        d['price'] = np.where(cond_2, '', d['price'])

        prices_ppa=SelectColumns(d,'hedge_id', 'projet_id', 'projet', 'type_hedge', 'date_debut', 
                                 'date_fin', 'date', 'année', 'trimestre', 'mois', 'price')
        return prices_ppa
        
    except Exception as e:
        print("")




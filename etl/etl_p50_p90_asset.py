import pandas as pd
import numpy as np
from datetime import datetime
import datetime as dt
xrange = range
import configparser
import sys
import os
pd.options.mode.chained_assignment = None
os.chdir('D:/local-repo-github/enr_portfolio_modeling/')
from functions import*
#Load Config
config_file=os.path.join(os.path.dirname("__file__"), 'Config/config.ini') 
config=configparser.ConfigParser(allow_no_value=True)
config.read(config_file)

dest_dir=os.path.join(os.path.dirname("__file__"), config['develop']['dest_dir'])
temp_dir=os.path.join(os.path.dirname("__file__"),config['develop']['temp_dir'])
         
def extract(prod_path, prod_pct_path, mean_pct_path, asset_path):
    ''' Function to extract excel files.
    Parameters
    ==========
    prod_path: str
        prod excel file path
    prod_pct_path: str
        prod excel file path
    mean_pct_path: str
        prod excel file path
    asset_path: str
        asset template file path
            
    Returns
    =======
    df_prod: DataFrame
        prod dataframe
    df_prod_pct: DataFrame
        prod profile dataframe
    df_mean_pct: DataFrame
        mean profile dataframe
    df_asset: DataFrame
        asset dataframe
    '''
    try:
        df_prod=ReadExcelFile(prod_path, sheet_name="productible")
        df_prod_pct=ReadExcelFile(prod_pct_path, sheet_name="profile_id")
        df_mean_pct=ReadExcelFile(mean_pct_path, sheet_name="mean_profile")
        df_asset=ReadExcelFile(asset_path)
        sub_df_asset=ReadExcelFile(asset_path, usecols=['asset_id', 'projet_id', 'technologie', 
                                                        'cod', 'puissance_installée', 'date_merchant', 
                                                        'date_dementelement', 'en_planif'])
        return df_prod, df_prod_pct, df_mean_pct, df_asset, sub_df_asset
    except Exception as e:
        print("Data Extraction error!: "+str(e))


def transform_asset(data_prod, mean_pct, **kwargs):
    """
    Function to compute P50 & p90 of asset in production    
    Parameters
    ==========
    data_prod (DataFrame) : Productibles, annual P50, P90 assets in production
    **kwargs : keyworded arguments
    data (DataFrame) : Sub-set of data of asset in production 
    a (int) : Takes the value 0
    b (int) : Takes the value of the length of our horizon (12*7)
    profile_pct (dictionaries) : Production profile prod_pct
    n_prod (int) : The arg takes the value length of data 
    date (str) : The arg takes the value of date colum label 'date'
    Returns
    =======
    asset_vmr_planif: DataFrame
        pandas df containing p50_p90 of asset in prod & planif
    """
    try:
        print('\n')
        print('compute p50_p90 Asset starts!:\n')
        print('here we go:\n')
        sub_asset=kwargs['sub_asset']
        sub_asset=sub_asset.loc[sub_asset["en_planif"]=="Non"]
        sub_asset=sub_asset.merge(data_prod, on='projet_id')
        sub_asset.reset_index(drop=True, inplace=True)
        n_prod=len(sub_asset) 
        prod_profile=kwargs['profile'].rename(columns=data_prod.set_index('projet_id')['projet'])
        prod_profile_dict=prod_profile.to_dict()
        
        print('creation df asset in prod starts!:\n')
        #This code is to compute monthly p50 and p90.  
        d=CreateDataFrame(sub_asset, '01-01-2022' , a=0, b=12*7, n=n_prod, date='date', 
                          profile=prod_profile_dict)    
        d["cod"]=pd.to_datetime(d["cod"])
        d["date"]=pd.to_datetime(d["date"])
        d["date_dementelement"]=pd.to_datetime(d["date_dementelement"])
        d["date_merchant"]=pd.to_datetime(d["date_merchant"])
        d['année']=d['date'].dt.year
        d['trim']=d['date'].dt.to_period('Q').dt.strftime("Q%q-%y")
        d['mois']=d['date'].dt.month
        #To remove p50, p90 based on cod and date_dementelement 
        results=RemoveP50P90(d, cod='cod', dd='date_dementelement', p50='p50_adj', 
                            p90='p90_adj', date='date', projetid='projet_id')
        
        asset_vmr= SelectColumns(results, 'asset_id', 'projet_id', 'projet', 
                                'date', 'année', 'trim', 'mois', 'p50_adj', 'p90_adj')
        
        print("creation df asset in prod ends!:\n")
        
        print("creation df asset in planif starts!:\n")
        asset=kwargs['asset']
        asset["date_merchant"].fillna(asset["cod"] + pd.DateOffset(years=20), inplace=True) 
        #To select only data with 2023 cod 
        filter = asset['cod'] > dt.datetime.today().strftime('%Y-%m-%d') 
        asset = asset.loc[filter]       
        asset.loc[asset['technologie']=='éolien', 'p50']=asset["puissance_installée"]*8760*0.25
        asset.loc[asset['technologie']=='éolien', 'p90']=asset["puissance_installée"]*8760*0.20
        asset.loc[asset['technologie']=='solaire', 'p50']=asset["puissance_installée"]*8760*0.15
        asset.loc[asset['technologie']=='solaire', 'p90']=asset["puissance_installée"]*8760*0.13
        #Select columns
        asset=SelectColumns(asset, "asset_id", "projet_id", "projet", "technologie", "cod", 
                            'date_dementelement','p50', 'p90')
        data_solar=asset.loc[asset['technologie'] == "solaire"]
        data_wp = asset.loc[asset['technologie'] == "éolien"]
        data_solar.reset_index(drop = True, inplace=True)
        data_wp.reset_index(drop = True, inplace=True)
        n_sol = len(data_solar) 
        n_wp = len(data_wp)
        mean_profile=mean_pct
        mean_profile_sol = mean_profile.iloc[:,[0, 1]]
        mean_profile_wp = mean_profile.iloc[:,[0,-1]]
        print('creation solar df starts!:\n')
        #create a df solar
        d1=CreateMiniDataFrame(data_solar, '01-01-2022', n=n_sol, a=0, b=12*7, date='date')   
        d1.reset_index(drop=True, inplace=True)
        mean_profile_sol=mean_profile_sol.assign(mth=[1 + i for i in xrange(len(mean_profile_sol))])[['mth'] + mean_profile_sol.columns.tolist()]
        #To calculate adjusted p50 and p90 solar adusted by the mean profile 
        s=mean_profile_sol.set_index('mth')['m_pct_solaire']
        pct = pd.to_datetime(d1['date']).dt.month.map(s)
        d1['p50_adj'] = -d1['p50'] * pct
        d1['p90_adj'] = -d1['p90'] * pct
                
        d1['cod'] = pd.to_datetime(d1['cod'])
        d1['date_dementelement'] = pd.to_datetime(d1['date_dementelement'])
        d1['date'] = pd.to_datetime(d1['date'])
        #To create new columns année, trimestre and mois
        d1['année'] = d1['date'].dt.year
        d1['trim'] = d1['date'].dt.to_period('Q').dt.strftime("Q%q-%y")
        d1['mois'] = d1['date'].dt.month
        #To remove p50, p90, based on cod and date_dementelement solar
        results=RemoveP50P90(d1, cod='cod', dd='date_dementelement', p50='p50_adj', 
                        p90='p90_adj', date='date', projetid='projet_id')
        asset_solar=SelectColumns(results, 'asset_id', 'projet_id', 'projet', 'date', 
                                'année', 'trim', 'mois', 'p50_adj', 'p90_adj')
        
        print('creation wind power df starts!:\n')
        #create a df wind power
        d2=CreateMiniDataFrame(data_wp, '01-01-2022', n=n_wp, a=0, b=12*7, date='date')   
        d2.reset_index(drop=True, inplace=True)
        #create a mth column containing number of month
        mean_profile_wp = mean_profile_wp.assign(mth=[1 + i for i in xrange(len(mean_profile_wp))])[['mth'] + mean_profile_wp.columns.tolist()]
        #To calculate adjusted p50 and p90 wp (adjusted with p50)
        s2 = mean_profile_wp.set_index('mth')['m_pct_eolien']
        pct = pd.to_datetime(d2['date']).dt.month.map(s2)
        d2['p50_adj'] = -d2['p50'] * pct
        d2['p90_adj'] = -d2['p90'] * pct
        d2["cod"] = pd.to_datetime(d2["cod"])
        d2['date_dementelement'] = pd.to_datetime(d2['date_dementelement'])
        d2["date"] = pd.to_datetime(d2["date"])
        #To create new columns année, trimestre and mois
        d2['année'] = d2['date'].dt.year
        d2['trim'] = d2['date'].dt.to_period('Q').dt.strftime("Q%q-%y")
        d2['mois'] = d2['date'].dt.month
        #To remove p50, p90, based on cod and date_dementelement wind power
        res=RemoveP50P90(d2, cod='cod', dd='date_dementelement', p50='p50_adj', 
                        p90='p90_adj', date='date', projetid='projet_id')
        
        asset_wp=SelectColumns(res, 'asset_id', 'projet_id', 'projet', 'date', 
                            'année', 'trim', 'mois', 'p50_adj', 'p90_adj')
        #To merge asset in prod and asset in planification
        asset_vmr_planif=MergeDataFrame(asset_vmr, asset_solar, asset_wp)
        asset_vmr_planif=asset_vmr_planif.assign(id=[1 + i for i in xrange(len(asset_vmr_planif))])[['id'] + asset_vmr_planif.columns.tolist()]
        print("creation df asset in planif ends:\n")
        print("Compute p50 p90 asset ends!")
        
        return asset_vmr_planif
 
    except Exception as e:
        print("Asset transformation error!: "+str(e))
        
def load(dest_dir, src_flow, file_name, file_extension):
    """Function to load data as excle file     
    parameters
    ==========
    dest_dir (str) :
        target folder path
    src_flow (DataFrame) :
        data frame returned by transform function        
    file_name (str) : 
        destination file name
    file_extension (str) :
        file extension as xlsx, csv, txt...
    exemple
    =======
    Load(dest_dir, template_asset_without_prod, 'template_asset', '.csv')
    >>> to load template_asset_without_prod in dest_dir as template_asset.csv 
    """
    try:
        if file_extension in ['.xlsx', '.xls', '.xlsm', '.xlsb', '.odf', '.ods', '.odt']:
            src_flow.to_excel(dest_dir+file_name+file_extension, index=False, float_format="%.4f")
        else: 
            src_flow.to_csv(dest_dir+file_name+file_extension, index=False, float_format="%.4f", encoding='utf-8-sig')
        print("Data loaded succesfully!")
    except Exception as e:
        print("Data load error!: "+str(e))
        


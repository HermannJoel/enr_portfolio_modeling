import pandas as pd
import os
#ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath('__file__'))) # This is your Project Root
os.chdir('D:/local-repo-github/enr_portfolio_modeling/')
from src.utils.functions import*

def extract_prices_planif(template_asset_path):
    try:
        df_hedge_=read_excel_file(template_hedge_path)
        return df_hedge_planif
    
    except Exception as e:
        print("Data extration error!: "+str(e))

def extract_prices_ppa(template_asset_path, ppa_path):
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
        df_template_asset = read_excel_file(template_asset_path)
        ppa = read_excel_file(ppa_path)
        
        return df_template_asset, df_ppa 
    except Exception as e:
        print("Data extraction error!: "+str(e))

def extract_prices_inprod(template_hedge_path, template_asset_path):
    ''' Function to extract excel files.
    Parameters
    ==========
    template_hedge_path: str
        path excel file containing data hedge in prod
    template_asset_path: str
        path excel file containing data hedge in planif    
    Returns
    =======
    df_template_hedge: DataFrame
        contracts prices asset in prod dataframe
    df_template_asset: DataFrame
    '''
    try:
        df_template_hedge = read_excel_file(template_hedge_path)
        df_template_asset = read_excel_file(template_asset_path)
        
        return df_template_hedge, df_template_asset 
    except Exception as e:
        print("Data Extraction error!: "+str(e))
        
        


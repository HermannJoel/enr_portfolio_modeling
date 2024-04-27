import pandas as pd
import os
import sys
sys.path.append('/mnt/d/local-repo-github/enr_portfolio_modeling')
#ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath('__file__'))) # This is your Project Root
os.chdir('/mnt/d/local-repo-github/enr_portfolio_modeling/')
from src.utils.functions import*



def extract_contract_prices(template_hedge_path, template_asset_path, ppa_path, template_prices_path):
    ''' Function to extract excel files.
    Parameters
    ==========
    template_hedge_path: str
        path excel file containing data template hedge 
    template_asset_path: str
        path excel file containing data template asset 
    ppa_path: str
        Excel file path containing data ppa
    Returns
    =======
    df_template_hedge: DataFrame
        contracts prices asset in prod dataframe
    df_template_asset: DataFrame
        
    df_ppa: DataFrame
        
    '''
    try:
        df_template_hedge = read_excel_file(template_hedge_path)
        df_template_asset = read_excel_file(template_asset_path)
        df_ppa = read_excel_file(ppa_path)
        df_prices = read_excel_file(template_prices_path)
        
        return df_template_hedge, df_template_asset, df_ppa, df_prices 
    except Exception as e:
        print("Data Extraction error!: "+str(e))
        
        


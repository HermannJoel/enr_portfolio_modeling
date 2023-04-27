import pandas as pd
import os
os.chdir('D:/local-repo-github/enr_portfolio_modeling/')
from src.utils.functions import*

def extract_vol_hedge(prod_path, prod_pct_path, mean_pct_path, asset_path, hedge_path):
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
    hedge_path: str
        hedge template excel file path
        
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
    df_hedge: DataFrame
        hedge dataframe
    '''
    df_prod = read_excel_file(prod_path, sheet_name="productible")
    df_prod_pct = read_excel_file(prod_pct_path, sheet_name="profile_id")
    df_mean_pct = read_excel_file(mean_pct_path, sheet_name="mean_profile")
    df_asset = read_excel_file(asset_path)
    df_hedge = read_excel_file(hedge_path)
    
    return df_prod, df_prod_pct, df_mean_pct, df_asset, df_hedge
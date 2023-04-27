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
from src.utils.functions import*


def extract_prod(prod_path, prod_pct_path, mean_pct_path, asset_path):
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
        df_prod = read_excel_file(prod_path, sheet_name="productible")
        df_prod_pct = read_excel_file(prod_pct_path, sheet_name="profile_id")
        df_mean_pct = read_excel_file(mean_pct_path, sheet_name="mean_profile")
        df_asset = read_excel_file(asset_path)
        sub_df_asset = read_excel_file(asset_path, usecols=['asset_id', 'projet_id', 'technologie', 
                                                        'cod', 'puissance_installée', 'date_merchant', 
                                                        'date_dementelement', 'en_planif'])
        return df_prod, df_prod_pct, df_mean_pct, df_asset, sub_df_asset
    except Exception as e:
        print("Data Extraction error!: "+str(e))
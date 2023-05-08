import pandas as pd
import os
os.chdir('D:/local-repo-github/enr_portfolio_modeling/')
from src.utils.functions import*

def extract_prices(prices_path, sub_df_template_asset):
    '''Function to extract excel files.
    Parameters
    ==========
    prices_path: str
        path excel file containing data hedge in prod
    template_asset_path: str
        path excel file containing data hedge in planif    
    Returns
    =======
    df_prices: DataFrame
        contracts prices asset in prod dataframe
    df_template_asset: DataFrame
        template asset dataframe
    '''
    try:
        df_prices=ReadExcelFile(prices_path, sheet_name='1-EO_Calcul Reporting', header=10)
        sub_df_template_asset=ReadExcelFile(template_asset_path, usecols = ["projet_id", "projet", "en_planif"])
        return df_prices, sub_df_template_asset 
    except Exception as e:
        print("Data Extraction error!: "+str(e))
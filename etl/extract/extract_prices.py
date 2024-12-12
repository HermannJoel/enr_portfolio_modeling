import pandas as pd
import os
import sys
sys.path.append('/mnt/d/local-repo-github/enr_portfolio_modeling')
os.chdir('/mnt/d/local-repo-github/enr_portfolio_modeling/')
from src.utils.functions import*
import logging.config

logger = logging.getLogger(__name__)

def extract_prices(prices_path, sub_template_asset_path):
    '''Function to extract excel files.
    Parameters
    ----------
    prices_path : str
        path excel file containing data hedge in prod
    template_asset_path : str
        path excel file containing data hedge in planif    
    Returns
    -------
    df_prices : DataFrame
        contracts prices asset in prod dataframe
    df_template_asset : DataFrame
        template asset dataframe
    '''
    logger.info("extract prices starts!")
    try:
        df_prices = read_excel_file(prices_path, sheet_name='1-EO_Calcul Reporting', header=10)
        sub_df_template_asset = read_excel_file(sub_template_asset_path, usecols = ["projet_id", "projet", "en_planif"]) 
    except Exception as e:
        logger.error(e)
        return
        print("extract prices error!: "+str(e))
        
    logger.info("extract prices ends!")
    return df_prices, sub_df_template_asset
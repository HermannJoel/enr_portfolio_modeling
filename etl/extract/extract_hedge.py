import pandas as pd
import os
import sys
sys.path.append('/mnt/d/local-repo-github/enr_portfolio_modeling')
os.chdir('/mnt/d/local-repo-github/enr_portfolio_modeling/')
from src.utils.functions import*
import logging.config

logger = logging.getLogger(__name__)

def extract_hedge(hedge_vmr_path, hedge_planif_path):
    '''Function to extract excel files.
    Parameters
    ----------
    hedge_vmr_path : str
        path excel file containing data hedge in prod
    hedge_planif_path : str
        path excel file containing data hedge in planif    
    Returns
    -------
    df_hedge_vmr : DataFrame
        hedge vmr dataframe
    df_hedge_planif : DataFrame
        hedge planif dataframe
    '''
    logger.info("extract hedge starts!")
    try:
        df_hedge_vmr = read_excel_file(hedge_vmr_path)
        df_hedge_planif = read_excel_file(hedge_planif_path) 
    except Exception as e:
        logger.error(e)
        return
        print("extract hedge error!: "+str(e))
        
    logger.info("extract hedge ends!")    
    return df_hedge_vmr, df_hedge_planif
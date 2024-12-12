import pandas as pd
import os
import sys
sys.path.append('/mnt/d/local-repo-github/enr_portfolio_modeling')
os.chdir('/mnt/d/local-repo-github/enr_portfolio_modeling/')
from src.utils.functions import*
import logging.config

logger = logging.getLogger(__name__)


def extract_asset(asset_vmr_path, asset_planif_path):
    '''Function to extract excel files.
    Parameters
    ----------
    asset_vmr_path : str
        path excel file containing data asset in prod
    asset_planif_path : str
        path excel file containing data asset in planif    
    Returns
    -------
    df_asset_vmr : DataFrame
        asset vmr dataframe
    df_planif : DataFrame
        asset planif dataframe
    '''
    logger.info("extract template asset starts!")
    try:
        df_asset_vmr = read_excel_file(asset_vmr_path, sheet_name="vmr", header=0)
        df_asset_planif=read_excel_file(asset_planif_path, sheet_name="Planification", header=20, 
                                      usecols=['#', 'Nom', 'Technologie', 'Puissance totale (pour les  repowering)', 
                                               'date MSI depl', "date d'entrée dans statut S", 'Taux de réussite']) 
    except Exception as e:
        logger.error(e)
        return
        print("data extraction error!: "+str(e))
        
    logger.info("extract template asset ends!")
    return df_asset_vmr, df_asset_planif

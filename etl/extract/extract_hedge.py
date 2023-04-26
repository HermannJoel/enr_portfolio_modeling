import pandas as pd
import os
os.chdir('D:/local-repo-github/enr_portfolio_modeling/')
from src.utils.functions import*

def extract_hedge(hedge_vmr_path, hedge_planif_path):
    ''' Function to extract excel files.
    Parameters
    ==========
    hedge_vmr_path: str
        path excel file containing data hedge in prod
    hedge_planif_path: str
        path excel file containing data hedge in planif    
    Returns
    =======
    df_hedge_vmr: DataFrame
        hedge vmr dataframe
    df_hedge_planif: DataFrame
        hedge planif dataframe
    '''
    try:
        df_hedge_vmr = read_excel_file(hedge_vmr_path)
        df_hedge_planif = read_excel_file(hedge_planif_path)
        return df_hedge_vmr, df_hedge_planif 
    except Exception as e:
        print("Data extraction error!: "+str(e))
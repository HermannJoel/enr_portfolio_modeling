import pandas as pd
import os
import sys
sys.path.append('/mnt/d/local-repo-github/enr_portfolio_modeling/')
os.chdir('/mnt/d/local-repo-github/enr_portfolio_modeling/')
from src.utils.functions import*

def extract_settlement_prices_data(future_prices_path:str, q_w_path:str, m_w_path:str):
    """ Function to extract excel files.
    Parameters
    ==========
    future_prices_path : str,
        path excel file containing data asset in prod
    q_w_path : str,
        path excel file containing data asset in planif 
    m_w_path : str,
    Returns
    =======
    calb : DataFrame,
    qb : DataFrame,
    mb : DataFrame,
    q_w : DataFrame,
    m_w : DataFrame
    """
    try:
        calb = read_excel_file(future_prices_path, sheet_name = 'YB')
        qb = read_excel_file(future_prices_path, sheet_name = 'QB')
        mb = read_excel_file(future_prices_path, sheet_name = 'MB')
        q_w = read_excel_file(q_w_path)
        m_w = read_excel_file(m_w_path)
        
        return calb, qb, mb, q_w, m_w 
    except Exception as e:
        print("Data extraction error!: "+str(e))
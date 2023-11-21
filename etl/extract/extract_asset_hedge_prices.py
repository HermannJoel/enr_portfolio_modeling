import pandas as pd
import os
import sys
sys.path.append('/mnt/d/local-repo-github/enr_portfolio_modeling')
os.chdir('/mnt/d/local-repo-github/enr_portfolio_modeling/')
from src.utils.functions import*


def extract_asset_hedge_prices(prod_asset_path, vol_hedge_path, template_hedge_path, template_asset_path, contract_prices_path, settl_prices_path):
    ''' Function to extract excel files.
    Parameters
    ==========
    prod_asset_path: str
        path excel file containing data asset in prod
    vol_hedge_path: str
        path excel file containing data asset in planif
    template_hedge_path : str
    
    template_asset_path : str
    
    contract_prices_path : str
    
    settl_prices_path : str
    Returns
    =======
    df_asset_vmr: DataFrame
        asset vmr dataframe
    df_planif: DataFrame
        asset planif dataframe
    '''
    try:
        df_prod_asset = rename_df_columns(df=read_excel_file(prod_asset_path, usecols=['asset_id', 'projet_id', 'date', 'année', 'trim', 'mois', 'p50_adj', 'p90_adj']), 
                                          column_names=['asset_id', 'projet_id', 'date', 'année', 'trim', 'mois', 'p50_a', 'p90_a'])
        df_vol_hedge=rename_df_columns(df=read_excel_file(vol_hedge_path, usecols=['hedge_id', 'projet_id', 'type_hedge', 'date', 'année', 'trim', 'mois', 'p50_adj', 'p90_adj']), 
                                       column_names=['hedge_id', 'projet_id', 'type_hedge_h', 'date', 'année', 'trim', 'mois', 'p50_h', 'p90_h'])
        df_template_hedge = read_excel_file(template_hedge_path, usecols=['hedge_id', 'projet_id', 'date_debut', 'date_fin'])
        df_template_asset = read_excel_file(template_asset_path, usecols=['asset_id', 'projet_id', 'cod', 'date_merchant', 'date_dementelement'])
        df_contract_prices = read_excel_file(contract_prices_path)
        df_settlement_prices = read_excel_file(settl_prices_path, usecols=['DeliveryPeriod', 'SettlementPrice'])
        
        return df_prod_asset, df_vol_hedge, df_template_hedge, df_template_asset, df_contract_prices, df_settlement_prices  
    except Exception as e:
        print("Data extraction error!: "+str(e))

import pandas as pd
import numpy as np
from datetime import datetime
import sys
import configparser
import os
sys.path.append('/mnt/d/local-repo-github/enr_portfolio_modeling/')
os.chdir('/mnt/d/local-repo-github/enr_portfolio_modeling/')
from src.utils.functions import*
from etl import*

config_file=os.path.join(os.path.dirname("__file__"), 'Config/config.ini') 
config=configparser.ConfigParser(allow_no_value=True)
config.read(config_file)

# Initialize Variables
profile=os.path.join(os.path.dirname("__file__"),config['develop']['profile'])
template_asset=os.path.join(os.path.dirname("__file__"),config['develop']['template_asset'])
template_hedge=os.path.join(os.path.dirname("__file__"),config['develop']['template_hedge'])
dest_dir = os.path.join(os.path.dirname("__file__"),config['develop']['processed_files_dir'])
template_price=os.path.join(os.path.dirname("__file__"),config['develop']['template_prices'])
prod_asset=os.path.join(os.path.dirname("__file__"),config['develop']['production_asset'])
vol_hedge=os.path.join(os.path.dirname("__file__"),config['develop']['volume_hedge'])
settl_prices_curve=os.path.join(os.path.dirname("__file__"),config['develop']['settl_prices_curve'])
contract_prices=os.path.join(os.path.dirname("__file__"),config['develop']['contract_prices'])

if __name__ == '__main__':
    df_prod_asset, df_vol_hedge, df_template_hedge, df_template_asset, df_contract_prices, df_settlement_prices=extract_asset_hedge_prices(prod_asset_path=prod_asset, 
                                                                                                                                           vol_hedge_path=vol_hedge, 
                                                                                                                                           template_hedge_path=template_hedge, 
                                                                                                                                           template_asset_path=template_asset, 
                                                                                                                                           contract_prices_path=contract_prices, 
                                                                                                                                           settl_prices_path=settl_prices_curve)
    df_modeled_settl_prices=model_settlement_prices(data_template_hedge=df_template_hedge, 
                                                data_settlement_prices=df_settlement_prices)
    df_modeled_settl_prices=rename_df_columns(df=df_modeled_settl_prices, column_names=["hedge_id", "projet_id", "date_debut", "date_fin", "date", "SettlementPrice"])
    #To merge p_50, P_90 asset and p_50, P90_hedge
    df_prod_aseet_vol_hedge=pd.merge(df_vol_hedge, df_prod_asset, how='left', on=['projet_id', 'date', 'année', 'trim', 'mois'])
    #To merge p_50, p_50, P90_hedge and contract prices(OA, CR, PPA)
    df_prod_aseet_vol_hedge_prices=pd.merge(df_prod_aseet_vol_hedge, df_contract_prices, how='left', on=['projet_id', 'hedge_id', 'date', 'année', 'mois'])
    df_prod_aseet_vol_hedge_prices.head()
    #To merge prod_prices with settlement prices
    df_modeled_settl_prices.head()
    df_prod_aseet_vol_hedge_contract_settl_prices=pd.merge(df_prod_aseet_vol_hedge_prices, df_modeled_settl_prices, how='left', on=['projet_id', 'hedge_id', 'date'])
    
    df_prod_aseet_vol_hedge_contract_settl_prices.head()

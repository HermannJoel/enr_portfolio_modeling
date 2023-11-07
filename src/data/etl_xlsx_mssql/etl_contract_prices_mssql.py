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

#Load Config
config_file=os.path.join(os.path.dirname("__file__"), 'Config/config.ini') 
config=configparser.ConfigParser(allow_no_value=True)
config.read(config_file)

src_dir = os.path.join(os.path.dirname("__file__"),config['develop']['src_dir'])
dest_dir = os.path.join(os.path.dirname("__file__"),config['develop']['processed_files_dir'])
asset = os.path.join(os.path.dirname("__file__"),config['develop']['template_asset'])
ppa = os.path.join(os.path.dirname("__file__"),config['develop']['ppa'])
hedge = os.path.join(os.path.dirname("__file__"),config['develop']['template_hedge'])
prices = os.path.join(os.path.dirname("__file__"),config['develop']['template_prices'])


if __name__ == '__main__':
    df_hedge, df_asset, df_ppa, df_prices = extract_contract_prices(template_hedge_path=hedge, template_asset_path=asset, ppa_path=ppa, template_prices_path=prices)
    prices_planif = transform_contract_prices_planif(data_hedge=df_hedge)
    prices_ppa = transform_contract_price_ppa(template_asset=df_asset, data_ppa=df_ppa)
    prices_oa_cr = transform_contract_prices_inprod(template_asset=df_asset, template_hedge=df_hedge, template_prices=df_prices , data_ppa=df_ppa)
    data_contract_prices = merge_data_frame(prices_planif, prices_ppa, prices_oa_cr)
    load_contract_prices_all(dest_dir = dest_dir, src_flow = data_contract_prices, file_name = 'contract_prices', file_extension = '.csv')
    
    
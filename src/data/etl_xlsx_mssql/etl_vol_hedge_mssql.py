import pandas as pd
import numpy as np
from datetime import datetime
import sys
import configparser
import os
os.chdir('D:/local-repo-github/enr_portfolio_modeling/')
from src.utils.functions import*
from etl import*

config_file=os.path.join(os.path.dirname("__file__"), 'Config/config.ini') 
config=configparser.ConfigParser(allow_no_value=True)
config.read(config_file)

# Initialize Variables
prod=os.path.join(os.path.dirname("__file__"),config['develop']['prod'])
asset=os.path.join(os.path.dirname("__file__"),config['develop']['asset'])
hedge=os.path.join(os.path.dirname("__file__"),config['develop']['hedge'])
val_dir = os.path.join(os.path.dirname("__file__"),config['develop']['ge_val_dir'])

if __name__ == '__main__':
    df_prod, df_profile, df_mean_profile, df_asset, df_hedge = extract_vol_hedge(prod_path = prod, prod_pct_path = prod, 
                                                                              mean_pct_path = prod, asset_path = asset, 
                                                                              hedge_path = hedge)
    df_oa, df_cr, df_ppa = transform_hedge_type(hedge = df_hedge)
    src_data = transform_vol_hedge(data_prod=df_prod, hedge=df_hedge, 
                                   prod_pct = df_profile, mean_pct = df_mean_profile, 
                                   oa=df_oa, cr=df_cr, ppa=df_ppa, profile=df_profile)
    load_vol_hedge(dest_dir = val_dir, src_flow=src_data, file_name="vol_hedge", file_extension='.csv')
    
    
    
    
    
    
    
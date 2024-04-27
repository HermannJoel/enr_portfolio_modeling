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

if __name__ == '__main__':
    df_prod, df_profile, df_mean_profile, df_asset, df_hedge = extract_vol_hedge(prod_path = profile, prod_pct_path = profile, 
                                                                              mean_pct_path = profile, asset_path = template_asset, 
                                                                              hedge_path = template_hedge)
    df_oa, df_cr, df_ppa = transform_hedge_type(hedge = df_hedge)
    src_data = transform_vol_hedge(data_prod=df_prod, hedge=df_hedge, 
                                   prod_pct = df_profile, mean_pct = df_mean_profile, 
                                   oa=df_oa, cr=df_cr, ppa=df_ppa, profile=df_profile)
    load_vol_hedge(dest_dir = dest_dir, src_flow=src_data, file_name="volume_hedge", file_extension='.csv')
    
    
    
    
    
    
    
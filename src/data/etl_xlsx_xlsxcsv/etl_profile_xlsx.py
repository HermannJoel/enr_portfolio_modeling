import pandas as pd
import sys
import os
import configparser
from datetime import datetime
sys.path.append('/mnt/d/local-repo-github/enr_portfolio_modeling/')
os.chdir('/mnt/d/local-repo-github/enr_portfolio_modeling/')
from src.utils.functions import*
from etl import*


#Load Config
config_file=os.path.join(os.path.dirname("__file__"), 'Config/config.ini') 
config=configparser.ConfigParser(allow_no_value=True)
config.read(config_file)

dest_dir=os.path.join(os.path.dirname("__file__"), config['develop']['dest_dir'])
temp_dir=os.path.join(os.path.dirname("__file__"),config['develop']['temp_dir'])
productibles=os.path.join(os.path.dirname("__file__"),config['develop']['productibles'])
project_names=os.path.join(os.path.dirname("__file__"),config['develop']['project_names'])
template_asset = os.path.join(os.path.dirname("__file__"),config['develop']['template_asset'])
val_dir = os.path.join(os.path.dirname("__file__"),config['develop']['ge_val_dir'])

if __name__ == '__main__':
    df_productibles, df_profile, df_project_names, df_template_asset = extract_profile(productible_path=productibles, 
                                                                                    project_names_path=project_names, 
                                                                                    template_asset_path=template_asset)
    
    df_prod, df_profile_id, df_profile, df_mean_profile, df_template_asset_with_prod = transform_prod_profile(data_productible=df_productibles, 
                                                                                                       data_profile=df_profile, 
                                                                                                       data_project_names=df_project_names,
                                                                                                       data_template_asset=df_template_asset)
    load_profile(dest_dir = val_dir, src_productible=df_prod, src_profile_id=df_profile_id, 
         src_profile=df_profile, src_mean_profile=df_mean_profile, file_name="profile")
    load_template_asset(dest_dir = val_dir, src_flow=df_template_asset_with_prod, file_name='asset', file_extension='.csv')
    
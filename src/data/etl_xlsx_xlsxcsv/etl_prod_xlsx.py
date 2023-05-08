import pandas as pd
import os
os.chdir('D:/local-repo-github/enr_portfolio_modeling/')
from src.utils.functions import*
from etl import*

#Load Config
config_file=os.path.join(os.path.dirname("__file__"), 'Config/config.ini') 
config=configparser.ConfigParser(allow_no_value=True)
config.read(config_file)

# Initialize Variables
prod=os.path.join(os.path.dirname("__file__"),config['develop']['prod'])
asset=os.path.join(os.path.dirname("__file__"),config['develop']['asset'])
hedge=os.path.join(os.path.dirname("__file__"),config['develop']['hedge'])
dest_dir=os.path.join(os.path.dirname("__file__"), config['develop']['dest_dir'])
temp_dir = os.path.join(os.path.dirname("__file__"),config['develop']['temp_dir'])
val_dir = os.path.join(os.path.dirname("__file__"),config['develop']['ge_val_dir'])

if __name__ == '__main__':
    df_prod, df_profile, df_mean_profile, df_asset, sub_df_asset = extract_prod(prod_path=prod, prod_pct_path=prod, mean_pct_path=prod, asset_path=asset)
    src_data = transform_prod_asset(data_prod=df_prod, mean_pct=df_mean_profile, prod_pct=df_profile, 
                                    sub_asset=sub_df_asset, profile=df_profile, asset=df_asset)
    load_prod(dest_dir = val_dir, src_flow = src_data, file_name='prod_asset', file_extension='.csv')
    
    
    
    
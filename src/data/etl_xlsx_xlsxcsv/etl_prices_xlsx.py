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


template_asset = os.path.join(os.path.dirname("__file__"),config['develop']['template_asset'])
data_prices = os.path.join(os.path.dirname("__file__"),config['develop']['prices'])
dest_dir = os.path.join(os.path.dirname("__file__"),config['develop']['processed_files_dir'])

if __name__ == '__main__':
    df_prices, sub_df_asset = extract_prices(prices_path = data_prices, sub_template_asset_path = template_asset)
    src_prices = transform_prices(data_prices = df_prices, sub_template_asset = sub_df_asset)
    load_prices_as_file(dest_dir = dest_dir, src_flow = src_prices, file_name = 'template_prices', file_extension = '.csv')
    
    
    

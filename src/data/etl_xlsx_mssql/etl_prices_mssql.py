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

dest_dir = os.path.join(os.path.dirname("__file__"),config['develop']['processed_files_dir'])
asset = os.path.join(os.path.dirname("__file__"),config['develop']['asset'])
prices = os.path.join(os.path.dirname("__file__"),config['develop']['prices'])


if __name__ == '__main__':
    df_prices, sub_df_asset = extract_prices(prices_path = prices, sub_template_asset_path = asset)
    src_data = transform_prices(data_prices = df_prices, sub_template_asset = sub_df_asset)
    load_prices_as_file(dest_dir = dest_dir, src_flow = src_data, file_name = 'template_prices', file_extension = '.csv')
    
    
    

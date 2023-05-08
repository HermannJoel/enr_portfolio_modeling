import sys
import configparser
import os
os.chdir('D:/local-repo-github/enr_portfolio_modeling/')
from src.utils.functions import*
from etl import*

#Load Config
config_file=os.path.join(os.path.dirname("__file__"), 'Config/config.ini') 
config=configparser.ConfigParser(allow_no_value=True)
config.read(config_file)

dest_dir = os.path.join(os.path.dirname("__file__"),config['develop']['dest_dir'])
asset = os.path.join(os.path.dirname("__file__"),config['develop']['asset'])
prices = os.path.join(os.path.dirname("__file__"),config['develop']['prices'])
processed_files_dir = os.path.join(os.path.dirname("__file__"),config['develop']['processed_files_dir'])

if __name__ == '__main__':
    df_prices, sub_df_asset = extract_prices(prices_path, sub_df_template_asset)
    process_prices = transform_prices(data_prices = df_prices, sub_template_asset = sub_df_asset)
    load_prices_as_file(dest_dir = processed_files_dir, src_flow = process_prices, file_name = 'template_contract_prices', file_extension = '.csv')
    
    
    

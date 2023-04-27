import pandas as pd
import numpy as np
from datetime import datetime
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

src_dir=os.path.join(os.path.dirname("__file__"),config['develop']['src_dir'])
dest_dir=os.path.join(os.path.dirname("__file__"),config['develop']['dest_dir'])
template_asset=os.path.join(os.path.dirname("__file__"),config['develop']['asset'])
ppa=os.path.join(os.path.dirname("__file__"),config['develop']['ppa'])
template_hedge=os.path.join(os.path.dirname("__file__"),config['develop']['hedge'])
template_prices=os.path.join(os.path.dirname("__file__"),config['develop']['prices'])


if __name__ == '__main__':
    extract_prices_planif(template_asset_path)
    extract_prices_ppa(template_asset_path, ppa_path)
    extract_prices_inprod(template_hedge_path, template_asset_path)
    transform_contract_prices_planif(self, df_hedge=None)
    transform_contract_price_ppa(template_asset, df_ppa, **kwargs)
    transform_contract_prices_inprod(template_asset, df_ppa, **kwargs)
    
    
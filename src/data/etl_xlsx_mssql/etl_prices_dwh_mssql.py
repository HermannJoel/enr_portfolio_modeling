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
template_asset = os.path.join(os.path.dirname("__file__"),config['develop']['template_asset'])
data_prices = os.path.join(os.path.dirname("__file__"),config['develop']['prices'])


if __name__ == '__main__':
import pytest
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
config_file=os.path.join(os.path.dirname('__file__'), 'Config/config.ini') 
config=configparser.ConfigParser(allow_no_value=True)
config.read(config_file)

# profile=read_excel_file(os.path.join(os.path.dirname('__file__'), config['develop']['profile']), sheet_name="productible")
# template_asset=read_excel_file(os.path.join(os.path.dirname('__file__'), config['develop']['template_asset']))
# template_hedge=read_excel_file(os.path.join(os.path.dirname('__file__'), config['develop']['template_hedge']))
# volume_hedge=read_excel_file(os.path.join(os.path.dirname('__file__'), config['develop']['volume_hedge']))
# production__asset=read_excel_file(os.path.join(os.path.dirname('__file__'), config['develop']['production_asset']))
# contract_prices=read_excel_file(os.path.join(os.path.dirname('__file__'), config['develop']['contract_prices']))

@pytest.fixture(scope="session", autouse=True)
def dataframe():
    dataframe=read_excel_file(os.path.join(os.path.dirname('__file__'), config['develop']['template_asset']))
    yield dataframe 

def test_col_exist_check(dataframe):
    label="projet_id"
    assert label in dataframe.columns
        
def test_is_unique_check(dataframe):
    assert pd.Series(dataframe["asset_id"]).is_unique
    

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
import logging.config

config_file=os.path.join(os.path.dirname("__file__"), 'Config/config.ini') 
config=configparser.ConfigParser(allow_no_value=True)
config.read(config_file)

log_file_path=os.path.join(os.path.dirname('__file__'), config['develop']['log_file_path'])
loging_timestamp = datetime.now().strftime('%Y%m%d%H%M%S')
logging.basicConfig(
    format='%(levelname)-8s:  %(asctime)s:  %(process)s:  %(funcName)s:  %(message)s',
    level=logging.INFO,
    datefmt='%Y-%m-%d %H:%M:%S',
    handlers=[ 
        logging.FileHandler(f"{log_file_path}/etl_hedge_{loging_timestamp}.log"),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)

hedge_vmr = os.path.join(os.path.dirname("__file__"),config['develop']['hedge_vmr'])
hedge_planif = os.path.join(os.path.dirname("__file__"),config['develop']['hedge_planif'])
raw_files = os.path.join(os.path.dirname("__file__"),config['develop']['raw_files_dir'])
dest_dir = os.path.join(os.path.dirname("__file__"),config['develop']['processed_files_dir'])
temp_dir = os.path.join(os.path.dirname("__file__"),config['develop']['temp_dir'])


if __name__ == '__main__':
    try:
        df_hedge_vmr, df_hedge_planif = extract_hedge(hedge_vmr_path=hedge_vmr, hedge_planif_path=hedge_planif)
        if df_hedge_vmr is None or df_hedge_planif is None: 
            raise Exception(f"extrat hedge failed: one or more dataframes are None")
        src_data = transform_hedge(hedge_vmr=df_hedge_vmr, hedge_planif=df_hedge_planif)
        if src_data is None:
            raise Exception(f"hedge transformation failed: src_data is None")
        load_hedge(dest_dir = dest_dir, src_flow=src_data, file_name="template_hedge_", file_extension='.csv')
    except Exception as e:
        logger.error(f"ETL Process Failed: {e}")
    

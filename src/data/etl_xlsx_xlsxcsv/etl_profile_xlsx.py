import pandas as pd
import sys
import os
import configparser
from datetime import datetime
sys.path.append('/mnt/d/local-repo-github/enr_portfolio_modeling/')
os.chdir('/mnt/d/local-repo-github/enr_portfolio_modeling/')
from src.utils.functions import*
from etl import*
import logging.config


#Load Config
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
        logging.FileHandler(f"{log_file_path}/etl_profile_{loging_timestamp}.log"),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)

dest_dir=os.path.join(os.path.dirname("__file__"), config['develop']['processed_files_dir'])
temp_dir=os.path.join(os.path.dirname("__file__"),config['develop']['temp_dir'])
productibles=os.path.join(os.path.dirname("__file__"),config['develop']['productibles'])
project_names=os.path.join(os.path.dirname("__file__"),config['develop']['project_names'])
asset = os.path.join(os.path.dirname("__file__"),config['develop']['asset'])


if __name__ == '__main__':
    try:
        df_productibles, df_profile, df_project_names, df_asset = extract_profile(productible_path=productibles, 
                                                                                  project_names_path=project_names, 
                                                                                  template_asset_path=asset)
        if df_productibles is None or df_profile is None or df_project_names is None or df_asset is None: 
            raise Exception(f"extrat profile failed: one or more dataframes are None")
        df_prod, df_profile_id, df_profile, df_mean_profile, df_template_asset_with_prod = transform_prod_profile(data_productible=df_productibles, 
                                                                                                                  data_profile=df_profile, 
                                                                                                                  data_project_names=df_project_names, 
                                                                                                                  data_template_asset=df_asset)
        if df_prod is None or df_profile_id is None or df_profile is None or df_mean_profile is None or df_template_asset_with_prod is none: 
            raise Exception(f"transform profile failed: one or more dataframes are None")
            
        load_profile(dest_dir = dest_dir, src_productible=df_prod, src_profile_id=df_profile_id, 
                     src_profile=df_profile, src_mean_profile=df_mean_profile, file_name="profile_")
        load_template_asset(dest_dir = dest_dir, src_flow=df_template_asset_with_prod, file_name='template_asset_', file_extension='.csv')
    except Exception as e:
        logger.error(f"ETL Process Failed: {e}")
    
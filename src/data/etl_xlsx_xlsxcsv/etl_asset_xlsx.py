import sys
import configparser
import os
import subprocess
import pandas as pd
# Call the shell script to change the working directory and run the Python script
#subprocess.run('.\cd_dir.sh', shell=True)
#repo_path = os.getenv('GITHUB_WORKSPACE', '/home/runner/work/enr_portfolio_modeling/enr_portfolio_modeling/')
#os.chdir(repo_path)
sys.path.append('/mnt/d/local-repo-github/enr_portfolio_modeling/')
os.chdir('/mnt/d/local-repo-github/enr_portfolio_modeling/')
from src.utils.functions import*
from etl import*

config_file=os.path.join(os.path.dirname('__file__'), 'Config/config.ini') 
config=configparser.ConfigParser(allow_no_value=True)
config.read(config_file)

log_file_path=os.path.join(os.path.dirname('__file__'), config['develop']['log_file_path'])
loging_timestamp = datetime.now().strftime('%Y%m%d%H%M%S')
logging.basicConfig(
    format='%(levelname)-8s:  %(asctime)s:  %(process)s:  %(funcName)s:  %(message)s',
    level=logging.INFO,
    datefmt='%Y-%m-%d %H:%M:%S',
    handlers=[ 
        logging.FileHandler(f"{log_file_path}/etl_asset_{loging_timestamp}.log"),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)

vmr = os.path.join(os.path.dirname('__file__'), config['develop']['vmr'])
planif = os.path.join(os.path.dirname('__file__'), config['develop']['planif'])
dest_dir = os.path.join(os.path.dirname('__file__'), config['develop']['processed_files_dir'])
temp_dir = os.path.join(os.path.dirname('__file__'),config['develop']['tempdir'])
val_dir = os.path.join(os.path.dirname('__file__'),config['develop']['ge_val_dir'])


if __name__ == '__main__':
    try:
        logger.info('ETL Process Initialized')
        df_asset_vmr, df_asset_planif = extract_asset(asset_vmr_path =vmr, asset_planif_path = planif)
        if df_asset_vmr is None or df_asset_planif is None:
            raise Exception(f"extrat asset failed: one or more dataframes are None")
        src_data = transform_asset(data_asset_vmr=df_asset_vmr, data_asset_planif=df_asset_planif)
        if src_data is None:
            raise Exception(f"asset transformation failed: src_data is None")
        load_asset(dest_dir = dest_dir, src_flow = src_data, file_name = 'asset', file_extension = '.csv')
    except Exception as e:
        logger.error(f"ETL Process Failed: {e}")
        
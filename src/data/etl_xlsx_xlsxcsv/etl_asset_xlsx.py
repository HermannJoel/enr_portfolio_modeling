import sys
import configparser
import os
import subprocess
# Call the shell script to change the working directory and run the Python script
#subprocess.run('.\cd_dir.sh', shell=True)
repo_path = os.getenv('GITHUB_WORKSPACE', '/home/runner/work/enr_portfolio_modeling/enr_portfolio_modeling/')
os.chdir(repo_path)
#sys.path.append('/mnt/d/local-repo-github/enr_portfolio_modeling/')
#os.chdir('/mnt/d/local-repo-github/enr_portfolio_modeling/')
from src.utils.functions import*
from etl import*
#Load Config
config_file=os.path.join(os.path.dirname('__file__'), 'Config/config.ini') 
config=configparser.ConfigParser(allow_no_value=True)
config.read(config_file)

vmr = os.path.join(os.path.dirname('__file__'), config['develop']['vmr'])
planif = os.path.join(os.path.dirname('__file__'), config['develop']['planif'])
dest_dir = os.path.join(os.path.dirname('__file__'), config['develop']['processed_files_dir'])
temp_dir = os.path.join(os.path.dirname('__file__'),config['develop']['tempdir'])
val_dir = os.path.join(os.path.dirname('__file__'),config['develop']['ge_val_dir'])

if __name__ == '__main__':
    df_asset_vmr, df_asset_planif = extract_asset(asset_vmr_path =vmr, asset_planif_path = planif)
    src_data = transform_asset(data_asset_vmr=df_asset_vmr, data_asset_planif=df_asset_planif)
    load_asset(dest_dir = dest_dir, src_flow = src_data, file_name = 'asset', file_extension = '.csv')

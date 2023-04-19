from data_pipelines.etl_template_asset import*
import pandas as pd
import configparser
import sys
import os
pd.options.mode.chained_assignment=None


#ChooseCwd(cwd='D:/git-local-cwd/Data-Engineering-Projects/') 
ChooseCwd(cwd=os.getcwd())
#Load Config    
config_file=os.path.join(os.path.dirname('__file__'), 'Config/config.ini') 
config=configparser.ConfigParser(allow_no_value=True)
config.read(config_file)

# Initialize Variables
vmr=os.path.join(os.path.dirname('__file__'), config['develop']['vmr'])
planif=os.path.join(os.path.dirname('__file__'), config['develop']['planif'])


if __name__=='__main__':
    df_asset_vmr, df_asset_planif=Extract(asset_vmr_path=vmr, asset_planif_path=planif)
    template_asset_without_prod=Transform(data_asset_vmr=df_asset_vmr, data_asset_planif=df_asset_planif)  
    Load(dest_dir=dest_dir, src_flow=template_asset_without_prod, file_name='template_asset', file_extension='.csv')
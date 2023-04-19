from data_pipelines.etl_p50_p90_asset import*
import pandas as pd
import os
import configparser
pd.options.mode.chained_assignment = None

#Load Config
config_file=os.path.join(os.path.dirname("__file__"), 'Config/config.ini') 
config=configparser.ConfigParser(allow_no_value=True)
config.read(config_file)

# Initialize Variables
prod=os.path.join(os.path.dirname("__file__"),config['develop']['prod'])
asset=os.path.join(os.path.dirname("__file__"),config['develop']['asset'])
hedge=os.path.join(os.path.dirname("__file__"),config['develop']['hedge'])


if __name__ == '__main__':
    df_prod, df_profile, df_mean_profile, df_asset, sub_df_asset=Extract(prod_path=prod, prod_pct_path=prod, 
                                                                      mean_pct_path=prod, asset_path=asset)
    p50_p90_asset=TransformAsset(data_prod=df_prod, prod_pct=df_profile, mean_pct=df_mean_profile,
                                 sub_asset=sub_df_asset, profile=df_profile, asset=df_asset)
    Load(dest_dir=dest_dir, src_flow=p50_p90_asset, file_name='p50_p90_asset', file_extension='.xlsx')
from data_pipelines.etl_volume_hedge import*
import pandas as pd
import configparser
import os
pd.options.mode.chained_assignment=None

#Load Config
config_file=os.path.join(os.path.dirname("__file__"), 'Config/config.ini')
config=configparser.ConfigParser(allow_no_value=True)
config.read(config_file)

# Initialize Variables
prod=os.path.join(os.path.dirname("__file__"),config['develop']['prod'])
asset=os.path.join(os.path.dirname("__file__"),config['develop']['asset'])
hedge=os.path.join(os.path.dirname("__file__"),config['develop']['hedge'])


if __name__ == '__main__':
    df_prod, df_profile, df_mean_profile, df_asset, df_hedge = extract(prod_path=prod, prod_pct_path=prod, 
                                                                mean_pct_path=prod, asset_path=asset, 
                                                                hedge_path=hedge)
    df_oa, df_cr, df_ppa = transform_(hedge=df_hedge)
    volume_hedge=TransformHedge(data_prod=df_prod, hedge=df_hedge, 
                                prod_pct=df_profile, mean_pct=df_mean_profile, 
                                oa=df_oa, cr=df_cr, ppa=df_ppa, profile=df_profile)
    Load(dest_dir=dest_dir, src_flow=volume_hedge, file_name="volume_hedge", file_extension='.xlsx')
    
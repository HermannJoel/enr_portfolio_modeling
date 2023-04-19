from data_pipelines.etl_template_prices import*
import pandas as pd
import os
import configparser

ChooseCwd(cwd=os.getcwd())
#Load Config
config_file=os.path.join(os.path.dirname("__file__"), 'Config/config.ini') 
config=configparser.ConfigParser(allow_no_value=True)
config.read(config_file)


template_asset=os.path.join(os.path.dirname("__file__"),config['develop']['template_asset'])
prices=os.path.join(os.path.dirname("__file__"),config['develop']['prices'])

if __name__ == '__main__':
    df_prices, sub_df_template_asset=Extract(prices_path=prices, template_asset_path=template_asset)
    template_prices=transform(data_prices=df_prices, sub_template_asset=sub_df_template_asset)
    Load(dest_dir=dest_dir, src_flow=template_prices, file_name="template_prices", file_extension='.xlsx')


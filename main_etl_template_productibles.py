from data_pipelines.etl_template_productibles import*
import pandas as pd
import os
import configparser
pd.options.mode.chained_assignment=None

#Load Config
config_file=os.path.join(os.path.dirname("__file__"), 'Config/config.ini') 
config=configparser.ConfigParser(allow_no_value=True)
config.read(config_file)

# Initialize Variables
productibles=os.path.join(os.path.dirname("__file__"),config['develop']['productibles'])
project_names=os.path.join(os.path.dirname("__file__"),config['develop']['project_names'])
template_asset=os.path.join(os.path.dirname("__file__"),config['develop']['template_asset'])

if __name__ == '__main__':
    df_productibles, df_profile, df_project_names, df_template_asset=Extract(productible_path=productibles, 
                                                                             project_names_path=project_names, 
                                                                             template_asset_path=template_asset)
    df_prod, df_profile_id, df_profile, df_mean_profile, df_template_asset_with_prod=Transform(data_productible=df_productibles, 
                                                                                               data_profile=df_profile, 
                                                                                               data_project_names=df_project_names,
                                                                                               data_template_asset=df_template_asset)
    Load(dest_dir=dest_dir, src_productible=df_prod, src_profile_id=df_profile_id, 
         src_profile=df_profile, src_mean_profile=df_mean_profile, file_name="template_prod")
    LoadTemplateAsset(dest_dir=dest_dir, src_flow=df_template_asset_with_prod, file_name='template_asset', file_extension='.csv')
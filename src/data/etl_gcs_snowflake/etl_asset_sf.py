import pandas as pd
import numpy as np
from datetime import datetime
import sys
import configparser
import os
os.chdir('D:/local-repo-github/enr_portfolio_modeling/')
from src.utils.functions import*
from etl import*

config_file=os.path.join(os.path.dirname("__file__"), 'Config/config.ini') 
config=configparser.ConfigParser(allow_no_value=True)
config.read(config_file)

hedge_vmr = os.path.join(os.path.dirname("__file__"),config['develop']['hedge_vmr'])
hedge_planif = os.path.join(os.path.dirname("__file__"),config['develop']['hedge_planif'])
src_dir = os.path.join(os.path.dirname("__file__"),config['develop']['src_dir'])
raw_files = os.path.join(os.path.dirname("__file__"),config['develop']['raw_files_dir'])
dest_dir = os.path.join(os.path.dirname("__file__"),config['develop']['dest_dir'])
processed_files = os.path.join(os.path.dirname("__file__"),config['develop']['processed_files_dir'])
temp_dir = os.path.join(os.path.dirname("__file__"),config['develop']['temp_dir'])
google_application_credentials = os.path.join(os.path.dirname("__file__"),config['develop']['google_application_credentials'])
datasetid = os.path.join(os.path.dirname("__file__"),config['develop']['datasetid']) #gbq stg ddb
tableid = os.path.join(os.path.dirname("__file__"),config['develop']['tableid'])
bucketid = os.path.join(os.path.dirname("__file__"),config['develop']['bucketid'])
mssqluid = os.path.join(os.path.dirname("__file__"),config['develop']['mssqluid'])
mssqlserver = os.path.join(os.path.dirname("__file__"),config['develop']['mssqlserver'])
msqsldriver = os.path.join(os.path.dirname("__file__"),config['develop']['mssqlserver'])
mssqldb = os.path.join(os.path.dirname("__file__"),config['develop']['mssqldw'])
uri = os.path.join(os.path.dirname("__file__"),config['develop']['uri'])
mongodbatlas_stg_conn_str = os.path.join(os.path.dirname("__file__"),config['develop']['mongodbatlas_stg_conn_str']) 
snowflake_user =  os.path.join(os.path.dirname("__file__"),config['develop']['snowflakeuser'])
snowflake_password = os.path.join(os.path.dirname("__file__"),config['develop']['snowflakepassword'])
snowflake_account =  os.path.join(os.path.dirname("__file__"),config['develop']['snowflakeaccount'])
snowflake_warehouse = os.path.join(os.path.dirname("__file__"),config['develop']['snowflakewarehouse'])
snowflake_schema = os.path.join(os.path.dirname("__file__"),config['develop']['snowflakeschema'])
sf_dest_table = os.path.join(os.path.dirname("__file__"),config['develop']['sf_dest_table'])
gcs_stage = os.path.join(os.path.dirname("__file__"),config['develop']['gcs_stage'])
gcs_stg_url = os.path.join(os.path.dirname("__file__"),config['develop']['gcs_stg_url'])


if __name__ == '__main__':
    df_asset_vmr, df_asset_planif = extract_asset(asset_vmr_path =vmr, asset_planif_path = planif)
    src_data = transform_asset(data_asset_vmr=df_asset_vmr, data_asset_planif=df_asset_planif)
    #load dimdate to blob gcs
    load_blob_to_gcs(source_file_name= processed_files+'asset.csv', 
                     bucket_name = bucketid, 
                     destination_blob_name = 'asset.csv', 
                     google_application_credentials = google_application_credentials)
    
    load_data_to_snowflake(snowflakeuser=snowflake_user, gcs_stg_url=gcs_stg_url,
                           snowflakepassword=snowflake_password, snowflakeaccount=snowflake_account, 
                           snowflakewarehouse=snowflake_warehouse, snowflakeschema=snowflake_schema,
                           sf_dest_table = sf_dest_table, gcs_stage = gcs_stage, 
                           query=f"COPY INTO {snowflake_schema}.{sf_dest_table} FROM @{snowflake_schema}.{gcs_stage} FILE_FORMAT = (FORMAT_NAME=MY_FILE_FORMAT) ON_ERROR='ABORT_STATEMENT';")
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

config_file=os.path.join(os.path.dirname("__file__"), 'Config/config.ini') 
config=configparser.ConfigParser(allow_no_value=True)
config.read(config_file)

# Initialize Variables
profile=os.path.join(os.path.dirname("__file__"),config['develop']['profile'])
template_asset=os.path.join(os.path.dirname("__file__"),config['develop']['template_asset'])
template_hedge=os.path.join(os.path.dirname("__file__"),config['develop']['template_hedge'])
dest_dir = os.path.join(os.path.dirname("__file__"),config['develop']['processed_files_dir'])
mongodbatlas_dw_conn_str = os.path.join(os.path.dirname("__file__"),config['develop']['mongodbatlas_dw_conn_str'])
mssqluid = os.path.join(os.path.dirname("__file__"),config['develop']['mssqluid'])
mssqlserver = os.path.join(os.path.dirname("__file__"),config['develop']['mssqlserver'])
msqsldriver = os.path.join(os.path.dirname("__file__"),config['develop']['mssqlserver'])
mssqldwhdb = os.path.join(os.path.dirname("__file__"),config['develop']['mssqldb'])
pgpw=os.path.join(os.path.dirname("__file__"),config['develop']['pgpw'])
pguid=os.path.join(os.path.dirname("__file__"),config['develop']['pguid'])
pgserver=os.path.join(os.path.dirname("__file__"),config['develop']['pgserver'])
pgdwhdb=os.path.join(os.path.dirname("__file__"),config['develop']['pgdwhdb'])
pgport=os.path.join(os.path.dirname("__file__"),config['develop']['pgport'])

if __name__ == '__main__':
    df_prod, df_profile, df_mean_profile, df_asset, df_hedge = extract_vol_hedge(prod_path = profile, prod_pct_path = profile, 
                                                                              mean_pct_path = profile, asset_path = template_asset, 
                                                                              hedge_path = template_hedge)
    df_oa, df_cr, df_ppa = transform_hedge_type(hedge = df_hedge)
    src_data = transform_vol_hedge(data_prod=df_prod, hedge=df_hedge, 
                                   prod_pct = df_profile, mean_pct = df_mean_profile, 
                                   oa=df_oa, cr=df_cr, ppa=df_ppa, profile=df_profile)
    load_vol_hedge(dest_dir = dest_dir, src_flow=src_data, file_name="volume_hedge", file_extension='.csv')
    load_docs_to_mongodb(dest_db='dw', dest_collection='VolumeHedge', 
                         src_data=src_data, 
                         date_format = '%Y-%m-%d', 
                         mongodb_conn_str = mongodbatlas_dw_conn_str)
    df_vol_hedge = read_docs_from_mongodb(src_db = 'dw', src_collection = 'VolumeHedge',
                                      query={}, no_id=True, 
                                      column_names=['HedgeId', 'ProjectId', 
                                                    'Project', 'TypeHedge','Date', 
                                                    'Year', 'Quarter', 'Month',
                                                    'P50', 'P90'], 
                                          mongodb_conn_str = mongodbatlas_dw_conn_str)
    df_hedge=read_docs_from_mongodb(src_db='dw', 
                                    src_collection='Hedge',  
                                    query={}, 
                                    no_id=True,
                                    column_names=["Id", "HedgeId", "AssetId", "ProjectId", "Project", "Technology", "TypeHedge", "ContractStartDate", 
                                                  "ContractEndDate", "DismantleDate", "InstalledPower", "InPlanif", "Profil", "HedgePct", 
                                                  "Countreparty", "CountryCountreparty"], 
                                    mongodb_conn_str=mongodbatlas_dw_conn_str) 
    src_data=sqldf("""select h."HedgeId", h."ProjectId", h."Project", vh."TypeHedge", vh."Date", vh."Year", vh."Quarter", vh."Month", vh."P50", vh."P90" 
                      from df_vol_hedge vh  
                      inner join df_hedge h on vh.HedgeId=h.HedgeId and vh.ProjectId=h.ProjectId;""", locals())
    excucute_postgres_crud_ops(
        queries=[
        '''TRUNCATE TABLE stagging."VolumeHedge";'''],  
        pguid=pguid, 
        pgpw=pgpw, 
        pgserver=pgserver,
        pgport=5432,
        pgdb=pgdwhdb,
        params=None
        )
    src_data[["P50","P90"]] = src_data[["P50","P90"]].apply(pd.to_numeric, errors='coerce')
    load_data_in_postgres_table(src_data=src_data, dest_table='VolumeHedge', 
                                pguid=pguid, pgpw=pgpw, pgserver=pgserver,  
                                pgdb=pgdwhdb, schema='stagging', if_exists='append')
      
    
    
    
    
    
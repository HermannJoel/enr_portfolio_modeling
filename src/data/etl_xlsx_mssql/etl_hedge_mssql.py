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
val_dir = os.path.join(os.path.dirname("__file__"),config['develop']['ge_val_dir'])



if __name__ == '__main__':
    df_hedge_vmr, df_hedge_planif = extract_hedge(hedge_vmr_path=hedge_vmr, hedge_planif_path=hedge_planif)
    src_data = transform_hedge(hedge_vmr=df_hedge_vmr, hedge_planif=df_hedge_planif)
        load_docs_to_mongodb(dest_db='staging', dest_collection='Hedge', 
                         src_data= src_data, 
                         date_format = '%Y-%m-%d', 
                         mongodb_conn_str = mongodbatlas_stg_conn_str)
    src_data = read_docs_from_mongodb(src_db = 'staging', src_collection = 'Hedge',
                                      query={}, no_id=True, 
                                      column_names=['Id','HedgeId', 'ProjetId', 
                                                    'Projet', 'TypeHedge', 
                                                    'ContractStartDate', 
                                                    'ContractEndDate','Profil', 
                                                    'HedgePct', 'Counterparty', 
                                                    'CountryCounterparty'], 
                                      mongodb_conn_str = mongodbatlas_stg_conn_str)
    src_data = src_data.fillna(value=0)
    load_data_to_mssql(src_data = src_data, dest_table = 'DimHedge', mssqlserver = mssqlserver, 
                       mssqldb = mssqldb, if_exists = 'replace', schema = 'dwh')
    

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

#Load Config
config_file=os.path.join(os.path.dirname("__file__"), 'Config/config.ini') 
config=configparser.ConfigParser(allow_no_value=True)
config.read(config_file)

src_dir = os.path.join(os.path.dirname("__file__"),config['develop']['src_dir'])
dest_dir = os.path.join(os.path.dirname("__file__"),config['develop']['processed_files_dir'])
template_asset = os.path.join(os.path.dirname("__file__"),config['develop']['template_asset'])
ppa = os.path.join(os.path.dirname("__file__"),config['develop']['ppa'])
template_hedge = os.path.join(os.path.dirname("__file__"),config['develop']['template_hedge'])
template_prices = os.path.join(os.path.dirname("__file__"),config['develop']['template_prices'])
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
    df_hedge, df_asset, df_ppa, df_prices = extract_contract_prices(template_hedge_path=template_hedge, template_asset_path=template_asset, ppa_path=ppa, template_prices_path=template_prices)  
    prices_planif = transform_contract_prices_planif(data_hedge=df_hedge)
    prices_ppa = transform_contract_price_ppa(template_asset=df_asset, data_ppa=df_ppa)
    prices_oa_cr = transform_contract_prices_inprod(template_asset=df_asset, template_hedge=df_hedge, template_prices=df_prices , data_ppa=df_ppa)
    data_contract_prices = merge_data_frame(prices_planif, prices_ppa, prices_oa_cr)
    load_contract_prices_all(dest_dir = dest_dir, src_flow = data_contract_prices, file_name = 'contract_prices', file_extension = '.csv')
    
    load_docs_to_mongodb(dest_db='dw', dest_collection='ContractPrices', 
                         src_data= data_contract_prices, 
                         date_format = '%Y-%m-%d', 
                         mongodb_conn_str = mongodbatlas_dw_conn_str)
    src_flow = read_docs_from_mongodb(src_db = 'dw', src_collection = 'ContractPrices',
                                      query={}, no_id=True, 
                                      column_names=['HedgeId', 'ProjectId', 
                                                    'Project', 'TypeHedge','Date', 
                                                    'Year', 'Quarter', 'Month',
                                                    'ContractPrice'], 
                                      mongodb_conn_str = mongodbatlas_dw_conn_str)
    excucute_postgres_crud_ops(
        queries=[
        '''TRUNCATE TABLE stagging."ContractPrices";'''],  
        pguid=pguid, 
        pgpw=pgpw, 
        pgserver=pgserver,
        pgport=5432,
        pgdb=pgdwhdb,
        params=None
        )
    src_flow["ContractPrice"] = src_flow["ContractPrice"].apply(pd.to_numeric, errors='coerce')
    load_data_in_postgres_table(src_data=src_flow, dest_table='ContractPrices', 
                                pguid=pguid, pgpw=pgpw, pgserver=pgserver,  
                                pgdb=pgdwhdb, schema='stagging', if_exists='append')
    
    src_flow=query_data_from_postgresql(query='''SELECT * FROM "stagging"."ContractPrices";''', 
                                pguid=pguid, pgpw=pgpw, pgserver=pgserver, pgport=pgport, pgdb=pgdwhdb)
    load_data_in_postgres_table(src_data=src_flow, dest_table='FactContractPrices', 
                                pguid=pguid, pgpw=pgpw, pgserver=pgserver,  
                                pgdb=pgdwhdb, schema='dwh', if_exists='append')


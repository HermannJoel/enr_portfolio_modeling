import pandas as pd
import numpy as np
from datetime import datetime
from pandasql import sqldf
pysqldf=lambda q: sqldf(q, globals())
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

hedge_vmr = os.path.join(os.path.dirname("__file__"),config['develop']['hedge_vmr'])
hedge_planif = os.path.join(os.path.dirname("__file__"),config['develop']['hedge_planif'])
src_dir = os.path.join(os.path.dirname("__file__"),config['develop']['src_dir'])
raw_files = os.path.join(os.path.dirname("__file__"),config['develop']['raw_files_dir'])
dest_dir = os.path.join(os.path.dirname("__file__"),config['develop']['processed_files_dir'])
temp_dir = os.path.join(os.path.dirname("__file__"),config['develop']['tempdir'])
template_hedge = os.path.join(os.path.dirname("__file__"),config['develop']['template_hedge'])
mongodbatlas_dw_conn_str = os.path.join(os.path.dirname("__file__"),config['develop']['mongodbatlas_dw_conn_str'])
pgpw=os.path.join(os.path.dirname("__file__"),config['develop']['pgpw'])
pgport=os.path.join(os.path.dirname("__file__"),config['develop']['pgport'])
pguid=os.path.join(os.path.dirname("__file__"),config['develop']['pguid'])
pgserver=os.path.join(os.path.dirname("__file__"),config['develop']['pgserver'])
pgstgdb=os.path.join(os.path.dirname("__file__"),config['develop']['pgstgdb'])
pgdwhdb=os.path.join(os.path.dirname("__file__"),config['develop']['pgdwhdb'])
mssqlserver = os.path.join(os.path.dirname("__file__"),config['develop']['mssqlserver'])
mssqldb = os.path.join(os.path.dirname("__file__"),config['develop']['mssqldb'])
temp_dir = os.path.join(os.path.dirname("__file__"),config['develop']['tempdir'])

if __name__ == '__main__':
    df_hedge_vmr, df_hedge_planif = extract_hedge(hedge_vmr_path=hedge_vmr, hedge_planif_path=hedge_planif)
    src_data = transform_hedge(hedge_vmr=df_hedge_vmr, hedge_planif=df_hedge_planif)
    load_hedge(dest_dir = dest_dir, src_flow=src_data, file_name="template_hedge_", file_extension='.csv')
    src_data=read_excel_file(template_hedge)
    load_docs_to_mongodb(dest_db='dw', dest_collection='Hedge', 
                         src_data= src_data, 
                         date_format = '%Y-%m-%d', 
                         mongodb_conn_str = mongodbatlas_dw_conn_str)
    # src_data = read_docs_from_mongodb(src_db = 'dw', src_collection = 'Hedge',
    #                                   query={}, no_id=True, 
    #                                   column_names=['Id','HedgeId', 'AssetId','ProjectId', 
    #                                                 'Project', 'Technology', 'TypeHedge', 
    #                                                 'ContractStartDate', 'ContractEndDate', 'DismantleDate',
    #                                                 'InstalledPower', 'InPlanif', 'Profil', 
    #                                                 'HedgePct', 'Counterparty', 'CountryCounterparty'], 
    #                                   mongodb_conn_str = mongodbatlas_dw_conn_str)
    
    df_hedge=read_docs_from_mongodb(src_db='dw', 
                                    src_collection='Hedge',  
                                    query={}, 
                                    no_id=True,
                                    column_names=["Id_", "HedgeId", "AssetId", "ProjectId_", "Project_", "Technology_", "TypeHedge", "ContractStartDate", 
                                                  "ContractEndDate", "DismantleDate_", "InstalledPower_", "InPlanif_", "Profil", "HedgePct", 
                                                  "Countreparty", "CountryCountreparty"], 
                                    mongodb_conn_str=mongodbatlas_dw_conn_str                                     ) 
    df_asset=read_docs_from_mongodb(src_db='dw', 
                                    src_collection='Asset',  
                                    query={}, 
                                    no_id=True,
                                    column_names=[ 'Id', 'AssetId', 'ProjectId', 'Project', 'Technology', 'COD','MW', 'SuccessPct', 'InstalledPower', 
                                                  'EOH', 'DateMerchant','DismantleDate', 'Repowering', 'MsiDate', 'InPlanif', "p50","p90"], 
                                    mongodb_conn_str=mongodbatlas_dw_conn_str) 
    
    src_data=sqldf("""select h."Id", h."HedgeId", a."AssetId", h."ProjectId", h."Project", h."Technology", h."TypeHedge", h."ContractStartDate", 
            h."ContractEndDate", h."DismantleDate", h."InstalledPower", h."InPlanif", h."Profil", h."HedgePct", h."Countreparty", h."CountryCountreparty"
            from df_hedge h  
            inner join df_asset a on h.AssetId=a.AssetId and h.ProjectId_=a.ProjectId;""", locals())
    scd2=src_data.iloc[:,1:]
    excucute_postgres_crud_ops(
        queries=[
        '''TRUNCATE TABLE stagging."Hedge";'''],  
        pguid=pguid, 
        pgpw=pgpw, 
        pgserver=pgserver,
        pgport=pgport,
        pgdb=pgdwhdb,
        params=None
        )
    load_data_in_postgres_table(src_data=scd2, dest_table='Hedge', 
                                pguid=pguid, pgpw=pgpw, pgserver=pgserver,  
                                pgdb=pgdwhdb, schema='stagging', if_exists='append')


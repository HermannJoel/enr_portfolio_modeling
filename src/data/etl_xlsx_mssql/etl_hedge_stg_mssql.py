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

if __name__ == '__main__':
    src_data=read_excel_file(template_hedge)
    load_docs_to_mongodb(dest_db='dw', dest_collection='Hedge', 
                         src_data= src_data, 
                         date_format = '%Y-%m-%d', 
                         mongodb_conn_str = mongodbatlas_dw_conn_str
                        )
    df_hedge=read_docs_from_mongodb(src_db='dw', 
                                    src_collection='Hedge',  
                                    query={}, 
                                    no_id=True,
                                    column_names=["Id", "HedgeId", "AssetId", "ProjectId", "Project", "Technology", "TypeHedge", "ContractStartDate", 
                                                  "ContractEndDate", "DismantleDate", "InstalledPower", "InPlanif", "Profil", "HedgePct", 
                                                  "Counterparty", "CountryCounterparty"], 
                                    mongodb_conn_str=mongodbatlas_dw_conn_str
                                   ) 
    src_data=df_hedge.iloc[:,1:]
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
    load_data_in_postgres_table(src_data=src_data, dest_table='Hedge', 
                                pguid=pguid, pgpw=pgpw, pgserver=pgserver,  
                                pgdb=pgdwhdb, schema='stagging', if_exists='append')


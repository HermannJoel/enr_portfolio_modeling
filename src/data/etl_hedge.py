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
dest_dir = os.path.join(os.path.dirname("__file__"),config['develop']['dest_dir'])
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


if __name__ == '__main__':
    df_hedge_vmr, df_hedge_planif = extract_hedge(hedge_vmr_path=hedge_vmr, hedge_planif_path=hedge_planif)
    src_data = transform_hedge(hedge_vmr=df_hedge_vmr, hedge_planif=df_hedge_planif)
    load_hedge(dest_dir=dest_dir, src_flow=src_data, file_name="template_hedge", file_extension='.csv')
    
    load_docs_to_mongodb(dest_db='staging', dest_collection='Hedge', 
                         src_data= src_hedge, 
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
    load_data_to_mssql(src_data = src_data, dest_table = 'Hedge', mssqlserver = mssqlserver, 
                       mssqldb = mssqldb, if_exists = 'replace', schema = 'stg')
    
    load_blob_to_gcs(source_file_name = dest_dir+"template_hedge.csv", 
                     bucket_name = bucketid, destination_blob_name = 'template_hedge.csv',
                     google_application_credentials = google_application_credentials)
    
    load_data_to_gcbq_from_gcs(uri = uri,
                               table_name = tableid,
                               google_application_credentials = google_application_credentials,
                               write_disposition = 'WRITE_TRUNCATE',
                               schema=[
                                   bigquery.SchemaField('Id', 'INTEGER'),
                                   bigquery.SchemaField('HedgeId', 'INTEGER'),
                                   bigquery.SchemaField('ProjetId', 'STRING'),
                                   bigquery.SchemaField('Projet', 'STRING'),
                                   bigquery.SchemaField('TypeHedge', 'STRING'),
                                   bigquery.SchemaField('ContractStartDate', 'DATE'),
                                   bigquery.SchemaField('ContractEndDate', 'DATE'),
                                   bigquery.SchemaField('Profil', 'STRING'),
                                   bigquery.SchemaField('HedgePct', 'NUMERIC'),
                                   bigquery.SchemaField('Counterparty', 'STRING'),
                                   bigquery.SchemaField('CountryCounterparty', 'STRING')
                               ] 
                              )
import sys
import configparser
import os
os.chdir('D:/local-repo-github/enr_portfolio_modeling/')
from src.utils.functions import*
from etl import*

#Load Config
config_file=os.path.join(os.path.dirname("__file__"), 'Config/config.ini') 
config=configparser.ConfigParser(allow_no_value=True)
config.read(config_file)

vmr = os.path.join(os.path.dirname('__file__'), config['develop']['vmr'])
planif = os.path.join(os.path.dirname('__file__'), config['develop']['planif'])
dest_dir = os.path.join(os.path.dirname("__file__"), config['develop']['dest_dir'])
temp_dir = os.path.join(os.path.dirname("__file__"),config['develop']['temp_dir'])
val_dir = os.path.join(os.path.dirname("__file__"),config['develop']['ge_val_dir'])
mongodbatlas_stg_conn_str = os.path.join(os.path.dirname("__file__"),config['develop']['mongodbatlas_stg_conn_str'])
mssqluid = os.path.join(os.path.dirname("__file__"),config['develop']['mssqluid'])
mssqlserver = os.path.join(os.path.dirname("__file__"),config['develop']['mssqlserver'])
msqsldriver = os.path.join(os.path.dirname("__file__"),config['develop']['mssqlserver'])
mssqldb = os.path.join(os.path.dirname("__file__"),config['develop']['mssqldw'])


excucute_sqlserver_crud_ops(
    queries=[
        "USE ODS",
        "TRUNCATE TABLE ods.Hedge",
        '''INSERT INTO ods.Hedge(HedgeId, ProjetId, Projet, TypeHedge, ContractStartDate, ContractEndDate, Profil, HedgePct, Counterparty, CountryCounterparty) 
        VALUES (393, 'RON3', 'Ronchois 3', 'PPA', '2024-01-01', '2024-06-30', 'As Produced', 1, 'Axpo', 'France'), 
        (392, 'RON3', 'Ronchois 3', 'PPA', '2023-01-01', '2023-06-30', 'As Produced', 0.75, 'Axpo', 'France')'''
    ], 
    mssqlserver='DESKTOP-JDQLDT1\MSSQLSERVERDWH', 
    mssqldb='ODS')


if __name__ == '__main__':
    df_asset_vmr, df_asset_planif = extract_asset(asset_vmr_path =vmr, asset_planif_path = planif)
    src_data = transform_asset(data_asset_vmr=df_asset_vmr, data_asset_planif=df_asset_planif)
    load_docs_to_mongodb(dest_db='staging', dest_collection='Asset', 
                         src_data= src_data, 
                         date_format = '%Y-%m-%d', 
                         mongodb_conn_str = mongodbatlas_stg_conn_str)
    src_data = read_docs_from_mongodb(src_db = 'staging', src_collection = 'Asset',
                                      query={}, no_id=True, 
                                      column_names=['Id','HedgeId', 'ProjetId', 
                                                    'Projet', 'TypeHedge', 
                                                    'ContractStartDate', 
                                                    'ContractEndDate','Profil', 
                                                    'HedgePct', 'Counterparty', 
                                                    'CountryCounterparty'], 
                                      mongodb_conn_str = mongodbatlas_stg_conn_str)
    src_data = src_data.fillna(value=0)
    load_data_to_mssql(src_data = src_data, dest_table = 'DimAsset', mssqlserver = mssqlserver, 
                       mssqldb = mssqldb, if_exists = 'replace', schema = 'dwh')
    
    





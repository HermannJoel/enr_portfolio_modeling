import pandas as pd
import sys
import os
import configparser
from datetime import datetime
sys.path.append('/mnt/d/local-repo-github/enr_portfolio_modeling/')
os.chdir('/mnt/d/local-repo-github/enr_portfolio_modeling/')
from src.utils.functions import*
from etl import*

#Load Config
config_file=os.path.join(os.path.dirname("__file__"), 'Config/config.ini') 
config=configparser.ConfigParser(allow_no_value=True)
config.read(config_file)

dest_dir=os.path.join(os.path.dirname("__file__"), config['develop']['processed_files_dir'])
temp_dir=os.path.join(os.path.dirname("__file__"),config['develop']['tempdir'])
productibles=os.path.join(os.path.dirname("__file__"),config['develop']['productibles'])
project_names=os.path.join(os.path.dirname("__file__"),config['develop']['project_names'])
asset = os.path.join(os.path.dirname("__file__"),config['develop']['asset'])
template_asset = os.path.join(os.path.dirname("__file__"),config['develop']['template_asset'])
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
    df_productibles, df_profile, df_project_names, df_template_asset_wo_prod = extract_profile(productible_path=productibles, 
                                                                                    project_names_path=project_names, 
                                                                                    template_asset_path=asset)
    
    df_prod, df_profile_id, df_profile, df_mean_profile, df_template_asset_with_prod = transform_prod_profile(data_productible=df_productibles,
                                                                                                              data_profile=df_profile, 
                                                                                                               data_project_names=df_project_names,
                                                                                                               #asset.csv template asset without prod
                                                                                                               data_template_asset=df_template_asset_wo_prod)
    load_profile(dest_dir = dest_dir, src_productible=df_prod, src_profile_id=df_profile_id, 
                 src_profile=df_profile, src_mean_profile=df_mean_profile, file_name="profile_")
    
    load_template_asset(dest_dir = dest_dir, src_flow=df_template_asset_with_prod, file_name='template_asset_', file_extension='.csv')
    template_asset=read_excel_file(template_asset)
    load_docs_to_mongodb(dest_db='dw', dest_collection='Asset', 
                         src_data=template_asset, 
                         date_format = '%Y-%m-%d', 
                         mongodb_conn_str = mongodbatlas_dw_conn_str)
    src_data = read_docs_from_mongodb(src_db = 'dw', src_collection = 'Asset',
                                      query={}, no_id=True, 
                                      column_names=['Id','AssetId', 'ProjectId', 
                                                    'Project', 'Technology','Cod', 
                                                    'MW', 'SuccessPct', 'InstalledPower',
                                                    'Eoh', 'DateMerchant', 'DismantleDate', 
                                                    'Repowering', 'DateMsi', 'InPlanif', 
                                                    'P50', 'P90'], 
                                      mongodb_conn_str = mongodbatlas_dw_conn_str)
    excucute_postgres_crud_ops(
        queries=[
        '''TRUNCATE TABLE stagging."Asset";'''],  
        pguid=pguid, 
        pgpw=pgpw, 
        pgserver=pgserver,
        pgport=5432,
        pgdb=pgdwhdb,
        params=None
        )
    load_data_in_postgres_table(src_data=src_data, dest_table='Asset', 
                                pguid=pguid, pgpw=pgpw, pgserver=pgserver,  
                                pgdb=pgdwhdb, schema='stagging', if_exists='append')
    
    scd1=query_data_from_postgresql(query='''SELECT "AssetId", "ProjectId", "Project", "Technology", "Cod",
                                                         "MW", "SuccessPct", "InstalledPower", "Eoh", "DateMerchant", "DismantleDate",
                                                         "Repowering", "DateMsi", "InPlanif", "P50", "P90" FROM "stagging"."Asset";''', 
                                    pguid=pguid, pgpw=pgpw, pgserver=pgserver, pgport=pgport, pgdb=pgdwhdb)   
    # load_data_in_postgres_table(src_data=scd1, dest_table="D_Asset", 
    #                              pguid=pguid, pgpw=pgpw, pgserver=pgserver,  
    #                              pgdb=pgdwhdb, schema='dwh', if_exists='append')
    # excucute_postgres_crud_ops(
    #      queries=[
    #      ''' INSERT INTO "stagging"."Asset"
    #      ("AssetId", "ProjectId", "Project", "Technology", "Cod", "MW", "SuccessPct", "InstalledPower", "Eoh", "DateMerchant",
    #      "DismantleDate", "Repowering", "DateMsi", "InPlanif", "P50", "P90")
    #      VALUES (1001, 'JB01', 'John Doe', 'éolien', '2023-02-01', 25.20, 1, 25.2, 
    #      0.000, '2043-02-01', '1901-01-01', 'None', '2023-02-01', True, 55188.000, 44150.400);'''],  
    #      pguid=pguid, 
    #      pgpw=pgpw, 
    #      pgserver=pgserver,
    #      pgport=5432,
    #      pgdb=pgdwhdb,
    #      params=None
    #      )

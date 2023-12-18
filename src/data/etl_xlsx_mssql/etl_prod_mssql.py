import pandas as pd
import os
import sys
sys.path.append('/mnt/d/local-repo-github/enr_portfolio_modeling/')
os.chdir('/mnt/d/local-repo-github/enr_portfolio_modeling/')
from src.utils.functions import*
from etl import*

#Load Config
config_file=os.path.join(os.path.dirname("__file__"), 'Config/config.ini') 
config=configparser.ConfigParser(allow_no_value=True)
config.read(config_file)

# Initialize Variables
profile=os.path.join(os.path.dirname("__file__"),config['develop']['profile'])
template_asset=os.path.join(os.path.dirname("__file__"),config['develop']['template_asset'])
template_hedge=os.path.join(os.path.dirname("__file__"),config['develop']['template_hedge'])
dest_dir=os.path.join(os.path.dirname("__file__"), config['develop']['processed_files_dir'])
temp_dir = os.path.join(os.path.dirname("__file__"),config['develop']['temp_dir'])
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
    df_prod, df_profile, df_mean_profile, df_asset, sub_df_asset = extract_prod(prod_path=profile, prod_pct_path=profile, 
                                                                                mean_pct_path=profile, asset_path=template_asset)

    src_data = transform_prod_asset(data_prod=df_prod, mean_pct=df_mean_profile, prod_pct=df_profile, 
                                    sub_asset=sub_df_asset, profile=df_profile, asset=df_asset)   
    load_prod(dest_dir = dest_dir, src_flow = src_data, file_name='production_asset', file_extension='.csv')
src_data.head()
    load_docs_to_mongodb(dest_db='dw', dest_collection='ProductionAsset', 
                         src_data=src_data, 
                         date_format = '%Y-%m-%d', 
                         mongodb_conn_str = mongodbatlas_dw_conn_str)
    df_prod_asset=read_docs_from_mongodb(src_db = 'dw', src_collection = 'ProductionAsset', 
                                         query={}, no_id=True, 
                                         column_names=['AssetId', 'ProjectId', 'Project', 
                                                        'Date', 'Year', 'Quarter', 
                                                        'Month', 'P50', 'P90'], 
                                      mongodb_conn_str = mongodbatlas_dw_conn_str)
    df_asset=read_docs_from_mongodb(src_db='dw', 
                                    src_collection='Asset',  
                                    query={}, 
                                    no_id=True,
                                    column_names=["Id", "AssetId", "ProjectId", "Project", "Technology", "Cod", "MW", 
                                                  "SuccessPct", "InstalledPower", "Eoh", "DateMerchant", "DismantleDate" , 
                                                  "Repowering", "DateMsi", "InPlanif", "P50", "P90"], 
                                    mongodb_conn_str=mongodbatlas_dw_conn_str) 
    src_data=sqldf("""select a."AssetId", a."ProjectId", a."Project", pa."Date", pa."Year", pa."Quarter", pa."Month", pa."P50", pa."P90" 
                      from df_prod_asset pa  
                      inner join df_asset a on pa.AssetId=a.AssetId and pa.ProjectId=a.ProjectId;""", locals())
    excucute_postgres_crud_ops(
        queries=[
        '''TRUNCATE TABLE stagging."ProductionAsset";'''],  
        pguid=pguid, 
        pgpw=pgpw, 
        pgserver=pgserver,
        pgport=5432,
        pgdb=pgdwhdb,
        params=None
        )
    src_data[["P50","P90"]] = src_data[["P50","P90"]].apply(pd.to_numeric, errors='coerce')
    load_data_in_postgres_table(src_data=src_data, dest_table='ProductionAsset', 
                                pguid=pguid, pgpw=pgpw, pgserver=pgserver,  
                                pgdb=pgdwhdb, schema='stagging', if_exists='append')
    excucute_postgres_crud_ops(queries=[
        '''UPDATE "stagging"."ProductionAsset" 
           SET "DateId" = to_char("Date", 'YYYYMMDD')::integer;'''], 
                                   pguid=pguid, 
                                   pgpw=pgpw, 
                                   pgserver=pgserver,
                                   pgport=pgport,
                                   pgdb=pgdwhdb,
                                   params=None)
    excucute_postgres_crud_ops(queries=[
        '''INSERT into dwh."I_Asset" (
        "AssetId", "DateId", "ProjectId", "Project", "Date", "Year", "Quarter", "Month", "P50A", "P90A") 
        select 
            pa."AssetId", pa."DateId", pa."ProjectId", pa."Project", pa."Date", pa."Year", pa."Quarter", pa."Month", pa."P50", pa."P90" 
            from stagging."ProductionAsset" as pa;'''], 
                                   pguid=pguid, 
                                   pgpw=pgpw, 
                                   pgserver=pgserver,
                                   pgport=pgport,
                                   pgdb=pgdwhdb,
                                   params=None)

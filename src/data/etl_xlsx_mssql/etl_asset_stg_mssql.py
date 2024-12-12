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

template_asset = os.path.join(os.path.dirname("__file__"),config['develop']['template_asset'])
mongodbatlas_stg_conn_str = os.path.join(os.path.dirname("__file__"),config['develop']['mongodbatlas_stg_conn_str'])
mongodbatlas_dw_conn_str = os.path.join(os.path.dirname("__file__"),config['develop']['mongodbatlas_dw_conn_str'])
mssqluid = os.path.join(os.path.dirname("__file__"),config['develop']['mssqluid'])
mssqlserver = os.path.join(os.path.dirname("__file__"),config['develop']['mssqlserver'])
msqsldriver = os.path.join(os.path.dirname("__file__"),config['develop']['mssqlserver'])
mssqldb = os.path.join(os.path.dirname("__file__"),config['develop']['mssqldb'])
pgpw=os.path.join(os.path.dirname("__file__"),config['develop']['pgpw'])
pgport=os.path.join(os.path.dirname("__file__"),config['develop']['pgport'])
pguid=os.path.join(os.path.dirname("__file__"),config['develop']['pguid'])
pgserver=os.path.join(os.path.dirname("__file__"),config['develop']['pgserver'])
pgstgdb=os.path.join(os.path.dirname("__file__"),config['develop']['pgstgdb'])
pgdwhdb=os.path.join(os.path.dirname("__file__"),config['develop']['pgdwhdb'])


if __name__ == '__main__':
    src_data=read_excel_file(template_asset)
    load_docs_to_mongodb(dest_db='dw', dest_collection='Asset', 
                         src_data= src_data, 
                         date_format = '%Y-%m-%d', 
                         mongodb_conn_str = mongodbatlas_dw_conn_str
                        )
    df_asset=read_docs_from_mongodb(src_db='dw', 
                                     src_collection='Asset',  
                                     query={}, 
                                     no_id=True,
                                     column_names=[ "Id", "AssetId", "ProjectId", "Project", "Technology", "Cod","MW", "SuccessPct", "InstalledPower", 
                                                   "Eoh", "DateMerchant","DismantleDate", "Repowering", "DateMsi", "InPlanif", "P50","P90"], 
                                     mongodb_conn_str=mongodbatlas_dw_conn_str
                                    )
    src_data=df_asset.iloc[:,1:]
    excucute_postgres_crud_ops(
        queries=[
        '''TRUNCATE TABLE stagging."Asset";'''],  
        pguid=pguid, 
        pgpw=pgpw, 
        pgserver=pgserver,
        pgport=pgport,
        pgdb=pgdwhdb,
        params=None
        )
    load_data_in_postgres_table(src_data=src_data, dest_table='Asset', 
                                pguid=pguid, pgpw=pgpw, pgserver=pgserver,  
                                pgdb=pgdwhdb, schema='stagging', if_exists='append')

    

    
    





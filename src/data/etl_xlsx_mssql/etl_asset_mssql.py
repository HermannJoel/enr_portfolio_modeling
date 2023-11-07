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

vmr = os.path.join(os.path.dirname('__file__'), config['develop']['vmr'])
planif = os.path.join(os.path.dirname('__file__'), config['develop']['planif'])
dest_dir = os.path.join(os.path.dirname("__file__"), config['develop']['processed_files_dir'])
temp_dir = os.path.join(os.path.dirname("__file__"),config['develop']['tempdir'])
val_dir = os.path.join(os.path.dirname("__file__"),config['develop']['ge_val_dir'])
mongodbatlas_stg_conn_str = os.path.join(os.path.dirname("__file__"),config['develop']['mongodbatlas_stg_conn_str'])
mongodbatlas_dw_conn_str = os.path.join(os.path.dirname("__file__"),config['develop']['mongodbatlas_dw_conn_str'])
mssqluid = os.path.join(os.path.dirname("__file__"),config['develop']['mssqluid'])
mssqlserver = os.path.join(os.path.dirname("__file__"),config['develop']['mssqlserver'])
msqsldriver = os.path.join(os.path.dirname("__file__"),config['develop']['mssqlserver'])
mssqldb = os.path.join(os.path.dirname("__file__"),config['develop']['mssqldb'])


if __name__ == '__main__':
    df_asset_vmr, df_asset_planif = extract_asset(asset_vmr_path =vmr, asset_planif_path = planif)
    src_data = transform_asset(data_asset_vmr=df_asset_vmr, data_asset_planif=df_asset_planif)
    load_asset(dest_dir = dest_dir, src_flow = src_data, file_name = 'asset', file_extension = '.csv')#load template_asset without prod
    # load_docs_to_mongodb(dest_db='dw', dest_collection='Asset', 
    #                      src_data= src_data, 
    #                      date_format = '%Y-%m-%d', 
    #                      mongodb_conn_str = mongodbatlas_dw_conn_str)
    # src_data = read_docs_from_mongodb(src_db = 'dw', src_collection = 'Asset',
    #                                   query={}, no_id=True, 
    #                                   column_names=['Id','AssetId', 'ProjetId', 
    #                                                 'Projet', 'Technology','COD', 
    #                                                 'MW', 'SuccessPct', 
    #                                                 'InstalledPower','EOH', 
    #                                                 'DateMerchant', 'DismantleDate', 
    #                                                 'Repowering', 'MsiDate', 'InPlanif'], 
    #                                   mongodb_conn_str = mongodbatlas_dw_conn_str)
    # src_data = src_data.fillna(value=0)
    # load_data_to_mssql(src_data = src_data, dest_table = 'Asset', mssqlserver = mssqlserver, 
    #                    mssqldb = mssqldb, if_exists = 'replace', schema = 'dwh')
    

    
    





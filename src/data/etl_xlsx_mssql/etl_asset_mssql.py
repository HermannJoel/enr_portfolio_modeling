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
    #execute scd1 to update dim asset
    excucute_sqlserver_crud_ops(
    queries=[
        ''' USE ODS ''',
        ''' MERGE [ods].[DimAsset] AS DST
            USING [ods].[Asset] AS SRC
            ON (SRC.AssetID = DST.AssetID AND SRC.ProjectID = DST.ProjectID)
            WHEN NOT MATCHED THEN
            INSERT (AssetID, ProjectID, Project, Technology, Cod, MW, SuccesPct, InstalledPower, Eoh, 
            DateMerchant, DismentleDate, Repowering, DateMsi, InPlanif, P50, P90)
            VALUES (SRC.AssetID, SRC.ProjectID, SRC.Project, SRC.Technology, SRC.Cod, SRC.MW, SRC.SuccesPct, SRC.InstalledPower, SRC.Eoh, 
            SRC.DateMerchant, SRC.DismentleDate, SRC.Repowering, SRC.DateMsi, SRC.InPlanif, SRC.P50, SRC.P90)
            WHEN MATCHED 
            AND (
            ISNULL(DST.Project,'') <> ISNULL(SRC.Project,'') 
             OR ISNULL(DST.Technology,'') <> ISNULL(SRC.Technology,'') 
             OR ISNULL(DST.Cod,'') <> ISNULL(SRC.Cod,'')
             OR ISNULL(DST.MW,'') <> ISNULL(SRC.MW,'')
             OR ISNULL(DST.SuccesPct,'') <> ISNULL(SRC.SuccesPct,'')
             OR ISNULL(DST.InstalledPower,'') <> ISNULL(SRC.InstalledPower,'')
             OR ISNULL(DST.Eoh,'') <> ISNULL(SRC.Eoh,'')
             OR ISNULL(DST.DateMerchant,'') <> ISNULL(SRC.DateMerchant,'')
             OR ISNULL(DST.DismentleDate,'') <> ISNULL(SRC.DismentleDate,'')
             OR ISNULL(DST.Repowering,'') <> ISNULL(SRC.Repowering,'')
             OR ISNULL(DST.DateMsi,'') <> ISNULL(SRC.DateMsi,'')
             OR ISNULL(DST.InPlanif,'') <> ISNULL(SRC.InPlanif,'')
             OR ISNULL(DST.P50,'') <> ISNULL(SRC.P50,'')
             OR ISNULL(DST.P90,'') <> ISNULL(SRC.P90,'')

             )
            THEN UPDATE 
            SET 
             DST.Project = SRC.Project 
             ,DST.Technology = SRC.Technology 
             ,DST.Cod = SRC.Cod
             ,DST.MW = SRC.MW
             ,DST.SuccesPct = SRC.SuccesPct
             ,DST.InstalledPower = SRC.InstalledPower
             ,DST.Eoh = SRC.Eoh
             ,DST.DateMerchant = SRC.DateMerchant 
             ,DST.DismentleDate = SRC.DismentleDate
             ,DST.Repowering = SRC.Repowering
             ,DST.DateMsi = SRC.DateMsi
             ,DST.InPlanif = SRC.InPlanif
             ,DST.P50 = SRC.P50
             ,DST.P90 = SRC.P90;'''
    ], 
    mssqlserver='DESKTOP-JDQLDT1\MSSQLSERVERDWH', 
    mssqldb='ODS')
    
    





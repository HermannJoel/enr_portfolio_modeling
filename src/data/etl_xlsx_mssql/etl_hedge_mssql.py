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

config_file=os.path.join(os.path.dirname("__file__"), 'Config/config.ini') 
config=configparser.ConfigParser(allow_no_value=True)
config.read(config_file)

hedge_vmr = os.path.join(os.path.dirname("__file__"),config['develop']['hedge_vmr'])
hedge_planif = os.path.join(os.path.dirname("__file__"),config['develop']['hedge_planif'])
src_dir = os.path.join(os.path.dirname("__file__"),config['develop']['src_dir'])
raw_files = os.path.join(os.path.dirname("__file__"),config['develop']['raw_files_dir'])
dest_dir = os.path.join(os.path.dirname("__file__"),config['develop']['processed_files_dir'])
temp_dir = os.path.join(os.path.dirname("__file__"),config['develop']['tempdir'])
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
    load_docs_to_mongodb(dest_db='dw', dest_collection='Hedge', 
                         src_data= src_data, 
                         date_format = '%Y-%m-%d', 
                         mongodb_conn_str = mongodbatlas_dw_conn_str)
    src_data = read_docs_from_mongodb(src_db = 'dw', src_collection = 'Hedge',
                                      query={}, no_id=True, 
                                      column_names=['Id','HedgeId', 'ProjectId', 
                                                    'Project', 'Technology', 'TypeHedge', 
                                                    'ContractStartDate', 'ContractEndDate', 'DismantleDate',
                                                    'InstalledPower', 'InPlanif', 'Profil', 
                                                    'HedgePct', 'Counterparty', 'CountryCounterparty'], 
                                      mongodb_conn_str = mongodbatlas_dw_conn_str)
    excucute_postgres_crud_ops(
        queries=[
        '''TRUNCATE TABLE stagging."Hedge";'''],  
        pguid=pguid, 
        pgpw=pgpw, 
        pgserver=pgserver,
        pgport=5432,
        pgdb=pgstgdb,
        params=None
        )
    
    load_data_in_postgres_table(src_data=src_data, dest_table='Hedge', 
                                pguid=pguid, pgpw=pgpw, pgserver=pgserver,  
                                pgdb=pgstgdb, schema='stagging', if_exists='append')
    
    src_scd2=query_data_from_postgresql(query='''SELECT * FROM "stagging"."Hedge";''',  
                                        pguid=pguid, pgpw=pgpw, pgserver=pgserver, pgport=pgport, pgdb=pgdwhdb)
    load_data_in_postgres_table(src_data=src_scd2, dest_table='DimHedge', 
                                pguid=pguid, pgpw=pgpw, pgserver=pgserver,  
                                pgdb=pgdwhdb, schema='dwh', if_exists='append')
    
    # excucute_sqlserver_crud_ops(
    #     queries=[''' TRUNCATE TABLE [stagging].[Hedge]; '''], 
    #     mssqlserver=mssqlserver, 
    #     mssqldb=mssqldb)
    # src_data = src_data[[ 'HedgeId', 'ProjetId', 'Projet',
    #                      'TypeHedge', 'ContractStartDate', 
    #                      'ContractEndDate', 'Profil','HedgePct', 
    #                      'Counterparty', 'CountryCounterparty']]
    # load_data_to_mssql(src_data = src_data, dest_table = 'Hedge', mssqlserver = mssqlserver, 
    #                    mssqldb = mssqldb, if_exists = 'append', schema = 'stagging')
    
#     excucute_sqlserver_crud_ops(
#     queries=[
#         ''' USE DWH;''',
#         ''' UPDATE [stagging].[Hedge] SET DimensionCheckSum = 
#         BINARY_CHECKSUM(HedgeId, ProjectId, Project, TypeHedge, ContractStartDate, ContractEndDate, Profil, 
#         HedgePct, Counterparty, CountryCounterparty);'''
#         ''' SET IDENTITY_INSERT [dwh].[DimHedge] ON 
#         INSERT INTO [dwh].[DimHedge]
#         ( ---Table and columns in which to insert the data
#           HedgeId,
#           ProjectId,
#           Project,
#           TypeHedge,
#           ContractStartDate,
#           ContractEndDate,
#           Profil,
#           HedgePct,
#           Counterparty,
#           CountryCounterparty,
#           DimensionCheckSum,
#           EffectiveDate,
#           EndDate
#           )
#         --- Select the rows/columns to insert that are output from this merge statement 
#         --- In this example, the rows to be inserted are the rows that have changed (UPDATE).
#         SELECT 
#             HedgeId,
#             ProjectId,
#             Project,
#             TypeHedge,
#             ContractStartDate,
#             ContractEndDate,
#             Profil,
#             HedgePct,
#             Counterparty,
#             CountryCounterparty,
#             DimensionCheckSum,
#             EffectiveDate,
#             EndDate
#         FROM
#         (
#         --- This is the beginning of the merge statement.
#         --- The target must be defined, in this example it is our slowly changing
#         --- dimension table 
#         MERGE into [dwh].[DimHedge] AS target 
#         --- The source must be defined with the USING clause 
#         USING 
#         (
#         --- The source is made up of the attribute columns from the staging table.
#         SELECT 
#             HedgeId,
#             ProjectId,
#             Project,
#             TypeHedge,
#             ContractStartDate,
#             ContractEndDate,
#             Profil,
#             HedgePct,
#             Counterparty,
#             CountryCounterparty,
#             DimensionCheckSum 
#         FROM [stagging].[Hedge]
#         ) 
#         AS source 
#         ( 
#             HedgeId,
#             ProjectId,
#             Project,
#             TypeHedge,
#             ContractStartDate,
#             ContractEndDate,
#             Profil,
#             HedgePct,
#             Counterparty,
#             CountryCounterparty,
#             DimensionCheckSum
#             ) 
#           ON ---We are matching on the SourceSystemID in the target table and the source table.
#           (
#             target.HedgeId = source.HedgeId AND target.ProjectId = source.ProjectId
#           )
#           --- If the ID's match but the CheckSums are different, then the record has changed;
#           --- therefore, update the existing record in the target, end dating the record 
#           --- and set the CurrentRecord flag to N
          
#           WHEN MATCHED and target.DimensionCheckSum <> source.DimensionCheckSum 
#           AND target.CurrentRecord='Y'
          
#           THEN 
#           UPDATE SET 
#           EndDate=GETDATE()-1, 
#           CurrentRecord='N', 
#           LastUpdated=GETDATE(), 
#           UpdatedBy=SUSER_SNAME()
          
#           WHEN NOT MATCHED THEN  
#           INSERT 
#           (
#           HedgeId,
#           ProjectId,
#           Project,
#           TypeHedge,
#           ContractStartDate,
#           ContractEndDate,
#           Profil,
#           HedgePct,
#           Counterparty,
#           CountryCounterparty,
#           DimensionCheckSum
#           )
#           VALUES 
#           (
#           source.HedgeId,
#           source.ProjectId,
#           source.Project,
#           source.TypeHedge,
#           source.ContractStartDate,
#           source.ContractEndDate,
#           source.Profil,
#           source.HedgePct,
#           source.Counterparty,
#           source.CountryCounterparty,
#           source.DimensionCheckSum
#           )
#           OUTPUT $ACTION, 
#           source.HedgeId,
#           source.ProjectId,
#           source.Project,
#           source.TypeHedge,
#           source.ContractStartDate,
#           source.ContractEndDate,
#           source.Profil,
#           source.HedgePct,
#           source.Counterparty,
#           source.CountryCounterparty,
#           source.DimensionCheckSum,
#           GETDATE(),
#           '12/31/9999'
#           ) --- the end of the merge statement
#           ---The changes output below are the records that have changed and will need
#           ---to be inserted into the slowly changing dimension.
#           AS CHANGES 
#           (
#           ACTION, 
#           HedgeId,
#           ProjectId,
#           Project,
#           TypeHedge,
#           ContractStartDate,
#           ContractEndDate,
#           Profil,
#           HedgePct,
#           Counterparty,
#           CountryCounterparty,
#           DimensionCheckSum,
#           EffectiveDate,
#           EndDate
#           )
#           WHERE ACTION ='UPDATE'; ''',
#           '''SET IDENTITY_INSERT [dwh].[DimHedge] OFF; '''],
#         mssqlserver=mssqlserver, 
#         mssqldb=mssqldb
#         )
    

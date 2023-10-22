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
dest_dir = os.path.join(os.path.dirname("__file__"),config['develop']['dest_dir'])
processed_files = os.path.join(os.path.dirname("__file__"),config['develop']['processed_files_dir'])
temp_dir = os.path.join(os.path.dirname("__file__"),config['develop']['temp_dir'])
val_dir = os.path.join(os.path.dirname("__file__"),config['develop']['ge_val_dir'])
mongodbatlas_dev_cluster = os.path.join(os.path.dirname("__file__"),config['develop']['mongodbatlas_stg_conn_str'])
mongodbatlas_test_cluster = os.path.join(os.path.dirname("__file__"),config['develop']['mongodbatlas_testcluster01nlazure'])

if __name__ == '__main__':
    df=read_data_from_mssql(
        query=''' SELECT * FROM [ods].[Hedge]'''
        , db='ODS', 
        server_instance='DESKTOP-JDQLDT1/MSSQLSERVERDWH')
    
    df=mongodb_crud_ops(mongodb_db = 'ods', mongodb_collection = 'Hedge', 
                     queries = 
                     [
                         'create_db ods'
                         'read_collection Hedge'
                     ], 
                     mongodb_cluster_conn = mongodbatlas_test_cluster)
    
    df_hedge_vmr, df_hedge_planif = extract_hedge(hedge_vmr_path=hedge_vmr, hedge_planif_path=hedge_planif)
    src_data = transform_hedge(hedge_vmr=df_hedge_vmr, hedge_planif=df_hedge_planif)
        load_docs_to_mongodb(dest_db='staging', dest_collection='Hedge', 
                         src_data= src_data, 
                         date_format = '%Y-%m-%d', 
                         mongodb_conn_str = mongodbatlas_dev_cluster)
    src_data = read_docs_from_mongodb(src_db = 'staging', src_collection = 'Hedge',
                                      query={}, no_id=True, 
                                      column_names=['Id','HedgeId', 'ProjetId', 
                                                    'Projet', 'TypeHedge', 
                                                    'ContractStartDate', 
                                                    'ContractEndDate','Profil', 
                                                    'HedgePct', 'Counterparty', 
                                                    'CountryCounterparty'], 
                                      mongodb_conn_str = mongodbatlas_dev_cluster)
    src_data = src_data.fillna(value=0)
    load_data_to_mssql(src_data = src_data, dest_table = 'DimHedge', mssqlserver = mssqlserver, 
                       mssqldb = mssqldb, if_exists = 'replace', schema = 'dwh')
    
    
    excucute_sqlserver_crud_ops(
        #--- Start of Day 1 - truncate ods.Hedge
        #--- insert a new record into the staging table into the staging table throught ssms
        #--- Update the checksum value in the staging table
        #--- begin of insert using merge
        #--- select * from ods.Hedge order by projectID
        #--- select * from ods.DimHedge order by projectID, hedgeID
        #--- Start of Day 2 - truncate ods.Hedge
        #--- insert a new record into the staging table
        #--- insert a changed record into the staging table
        #--- Update the checksum value in the staging table
        #--- begin of insert using merge
        #--- select * from ods.Hedge order by projectID
        #--- select * from ods.DimHedge order by projectID, hedgeID
    queries=[
        ''' USE ODS; ''',
        ''' TRUNCATE TABLE ods.Hedge; ''',
        ''' UPDATE ods.Hedge SET DimensionCheckSum = 
        BINARY_CHECKSUM(HedgeId, ProjetId, Projet, TypeHedge, ContractStartDate, ContractEndDate, Profil, 
        HedgePct, Counterparty, CountryCounterparty);'''
        ''' SET IDENTITY_INSERT [ods].[DimHedge] ON 
        INSERT INTO [ods].[DimHedge]
        ( ---Table and columns in which to insert the data
          HedgeId,
          ProjetId,
          Projet,
          TypeHedge,
          ContractStartDate,
          ContractEndDate,
          Profil,
          HedgePct,
          Counterparty,
          CountryCounterparty,
          DimensionCheckSum,
          EffectiveDate,
          EndDate
          )
        --- Select the rows/columns to insert that are output from this merge statement 
        --- In this example, the rows to be inserted are the rows that have changed (UPDATE).
        SELECT 
            HedgeId,
            ProjetId,
            Projet,
            TypeHedge,
            ContractStartDate,
            ContractEndDate,
            Profil,
            HedgePct,
            Counterparty,
            CountryCounterparty,
            DimensionCheckSum,
            EffectiveDate,
            EndDate
        FROM
        (
        --- This is the beginning of the merge statement.
        --- The target must be defined, in this example it is our slowly changing
        --- dimension table 
        MERGE into ods.DimHedge AS target 
        --- The source must be defined with the USING clause 
        USING 
        (
        --- The source is made up of the attribute columns from the staging table.
        SELECT 
            HedgeId,
            ProjetId,
            Projet,
            TypeHedge,
            ContractStartDate,
            ContractEndDate,
            Profil,
            HedgePct,
            Counterparty,
            CountryCounterparty,
            DimensionCheckSum 
        FROM ods.Hedge
        ) 
        AS source 
        ( 
            HedgeId,
            ProjetId,
            Projet,
            TypeHedge,
            ContractStartDate,
            ContractEndDate,
            Profil,
            HedgePct,
            Counterparty,
            CountryCounterparty,
            DimensionCheckSum
            ) 
          ON ---We are matching on the SourceSystemID in the target table and the source table.
          (
            target.HedgeId = source.HedgeId AND target.ProjetId = source.ProjetId
          )
          --- If the ID's match but the CheckSums are different, then the record has changed;
          --- therefore, update the existing record in the target, end dating the record 
          --- and set the CurrentRecord flag to N
          
          WHEN MATCHED and target.DimensionCheckSum <> source.DimensionCheckSum 
          AND target.CurrentRecord='Y'
          
          THEN 
          UPDATE SET 
          EndDate=GETDATE()-1, 
          CurrentRecord='N', 
          LastUpdated=GETDATE(), 
          UpdatedBy=SUSER_SNAME()
          --- If the ID's do not match, then the record is new;
          --- therefore, insert the new record into the target using the values from the source.
          WHEN NOT MATCHED THEN  
          INSERT 
          (
          HedgeId,
          ProjetId,
          Projet,
          TypeHedge,
          ContractStartDate,
          ContractEndDate,
          Profil,
          HedgePct,
          Counterparty,
          CountryCounterparty,
          DimensionCheckSum
          )
          VALUES 
          (
          source.HedgeId,
          source.ProjetId,
          source.Projet,
          source.TypeHedge,
          source.ContractStartDate,
          source.ContractEndDate,
          source.Profil,
          source.HedgePct,
          source.Counterparty,
          source.CountryCounterparty,
          source.DimensionCheckSum
          )
          OUTPUT $ACTION, 
          source.HedgeId,
          source.ProjetId,
          source.Projet,
          source.TypeHedge,
          source.ContractStartDate,
          source.ContractEndDate,
          source.Profil,
          source.HedgePct,
          source.Counterparty,
          source.CountryCounterparty,
          source.DimensionCheckSum,
          GETDATE(),
          '12/31/9999'
          ) --- the end of the merge statement
          ---The changes output below are the records that have changed and will need
          ---to be inserted into the slowly changing dimension.
          AS CHANGES 
          (
          ACTION, 
          HedgeId,
          ProjetId,
          Projet,
          TypeHedge,
          ContractStartDate,
          ContractEndDate,
          Profil,
          HedgePct,
          Counterparty,
          CountryCounterparty,
          DimensionCheckSum,
          EffectiveDate,
          EndDate
          )
          WHERE ACTION ='UPDATE'; ''';
          '''SET IDENTITY_INSERT [ods].[DimHedge] OFF; ''',
        
        ''' TRUNCATE TABLE ods.Hedge; ''',
        ''' INSERT INTO ods.Hedge(HedgeId, ProjetId, Projet, TypeHedge, 
        ContractStartDate, ContractEndDate, Profil, HedgePct, Counterparty, CountryCounterparty)
        VALUES (395, 'RON3', 'Ronchois 3', 'PPA', '2026-01-01', '2026-12-31', 'As Produced', 0.50, 'Tesla', 'Germany') ''',
        ''' INSERT INTO ods.Hedge (HedgeId, ProjetId, Projet, TypeHedge, 
        ContractStartDate, ContractEndDate, Profil, HedgePct, Counterparty, CountryCounterparty) 
        VALUES (393, 'RON3', 'Ronchois 3', 'PPA', '2024-01-01', '2024-06-30', 'As Produced', 1, 'Axpo', 'France'), 
        (392, 'RON3', 'Ronchois 3', 'PPA', '2023-01-01', '2023-06-30', 'As Produced', 0.75, 'Axpo', 'France') ''',
        ''' UPDATE ods.Hedge SET DimensionCheckSum = 
        BINARY_CHECKSUM(HedgeId, ProjetId, Projet, TypeHedge, ContractStartDate, 
        ContractEndDate, Profil, HedgePct, Counterparty, CountryCounterparty); ''',
        
        ''' SET identity_insert [ods].[DimHedge] ON ''',
        ''' INSERT INTO [ods].[DimHedge] 
        ( --Table and columns in which to insert the data
        HedgeId,
        ProjetId,
        Projet,
        TypeHedge,
        ContractStartDate,
        ContractEndDate,
        Profil,
        HedgePct,
        Counterparty,
        CountryCounterparty,
        DimensionCheckSum,
        EffectiveDate,
        EndDate
        )
        --- Select the rows/columns to insert that are output from this merge statement 
        --- In this example, the rows to be inserted are the rows that have changed (UPDATE).
        SELECT 
        HedgeId,
        ProjetId,
        Projet,
        TypeHedge,
        ContractStartDate,
        ContractEndDate,
        Profil,
        HedgePct,
        Counterparty,
        CountryCounterparty,
        DimensionCheckSum,
        EffectiveDate,
        EndDate
        
        FROM
        (
      -- This is the beginning of the merge statement.
      -- The target must be defined, in this example it is our slowly changing
      -- dimension table
      MERGE into ods.DimHedge AS target
      -- The source must be defined with the USING clause
      USING 
      (
      -- The source is made up of the attribute columns from the staging table.
      SELECT 
      HedgeId,
      ProjetId,
      Projet,
      TypeHedge,
      ContractStartDate,
      ContractEndDate,
      Profil,
      HedgePct,
      Counterparty,
      CountryCounterparty,
      DimensionCheckSum
    FROM ods.Hedge
    ) 
    AS source 
    ( 
      HedgeId,
      ProjetId,
      Projet,
      TypeHedge,
      ContractStartDate,
      ContractEndDate,
      Profil,
      HedgePct,
      Counterparty,
      CountryCounterparty,
      DimensionCheckSum
      ) ON --We are matching on the SourceSystemID in the target table and the source table.
      (
      target.HedgeId = source.HedgeId AND target.ProjetId = source.ProjetId
      )
      --- If the ID's match but the CheckSums are different, then the record has changed;
      --- therefore, update the existing record in the target, end dating the record 
      --- and set the CurrentRecord flag to N
      WHEN MATCHED AND target.DimensionCheckSum <> source.DimensionCheckSum 
      AND target.CurrentRecord='Y'
      THEN 
      UPDATE SET 
        EndDate=GETDATE()-1, 
        CurrentRecord='N', 
        LastUpdated=GETDATE(), 
        UpdatedBy=SUSER_SNAME()
      --- If the ID's do not match, then the record is new;
      --- therefore, insert the new record into the target using the values from the source.
      WHEN NOT MATCHED THEN  
      INSERT 
      (
      HedgeId,
      ProjetId,
      Projet,
      TypeHedge,
      ContractStartDate,
      ContractEndDate,
      Profil,
      HedgePct,
      Counterparty,
      CountryCounterparty,
      DimensionCheckSum
      )
      VALUES 
      (
      source.HedgeId,
      source.ProjetId,
      source.Projet,
      source.TypeHedge,
      source.ContractStartDate,
      source.ContractEndDate,
      source.Profil,
      source.HedgePct,
      source.Counterparty,
      source.CountryCounterparty,
      source.DimensionCheckSum
      )
      OUTPUT $ACTION, 
      source.HedgeId,
      source.ProjetId,
      source.Projet, 
      source.TypeHedge,
      source.ContractStartDate,
      source.ContractEndDate,
      source.Profil, 
      source.HedgePct, 
      source.Counterparty, 
      source.CountryCounterparty, 
      source.DimensionCheckSum, 
      GETDATE(),
      '12/31/9999'
      ) --- the end of the merge statement
      ---The changes output below are the records that have changed and will need 
      ---to be inserted into the slowly changing dimension. 
      AS CHANGES 
      ( 
      ACTION, 
      HedgeId,
      ProjetId,
      Projet,
      TypeHedge,
      ContractStartDate,
      ContractEndDate,
      Profil,
      HedgePct,
      Counterparty,
      CountryCounterparty,
      DimensionCheckSum,
      EffectiveDate,
      EndDate 
      ) 
      WHERE ACTION ='UPDATE'; ''', 
    ''' SET IDENTITY_INSERT [ods].[DimHedge] OFF ''',
    ], 
    mssqlserver='DESKTOP-JDQLDT1\MSSQLSERVERDWH', 
    mssqldb='ODS')
    

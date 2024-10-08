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
    excucute_postgres_crud_ops(
         queries=[
         '''INSERT INTO "dwh"."D_Asset" ("AssetId", "ProjectId", "Project", "Technology", "Cod", "MW", 
         "SuccessPct", "InstalledPower", "Eoh", "DateMerchant", "DismantleDate", "Repowering", "DateMsi", "InPlanif", "P50","P90")
         SELECT
         SRC."AssetId", SRC."ProjectId", SRC."Project", SRC."Technology", 
         SRC."Cod", SRC."MW", SRC."SuccessPct", SRC."InstalledPower", SRC."Eoh",
         SRC."DateMerchant", SRC."DismantleDate", SRC."Repowering", SRC."DateMsi", SRC."InPlanif", SRC."P50", SRC."P90" 
         FROM "stagging"."Asset" AS SRC
         ON CONFLICT ("AssetId", "ProjectId") DO UPDATE
         SET 
         "Project" = EXCLUDED."Project",
         "Technology" = EXCLUDED."Technology",
         "Cod" = EXCLUDED."Cod",
         "MW" = EXCLUDED."MW",
         "SuccessPct" = EXCLUDED."SuccessPct",
         "InstalledPower" = EXCLUDED."InstalledPower",
         "Eoh" = EXCLUDED."Eoh",
         "DateMerchant" = EXCLUDED."DateMerchant",
         "DismantleDate" = EXCLUDED."DismantleDate",
         "Repowering" = EXCLUDED."Repowering",
         "DateMsi" = EXCLUDED."DateMsi",
         "InPlanif" = EXCLUDED."InPlanif",
         "P50" = EXCLUDED."P50",
         "P90" = EXCLUDED."P90";'''],  
         pguid=pguid, 
         pgpw=pgpw, 
         pgserver=pgserver,
         pgport=pgport,
         pgdb=pgdwhdb,
         params=None
         )
    
#     load_data_to_mssql(src_data=src_data, dest_table='Asset', mssqlserver = mssqlserver, mssqldb=mssqldwhdb, if_exists='append', schema='stagging')

      #execute scd1 to update dim_asset
#     excucute_sqlserver_crud_ops(
#     queries=[
#         ''' USE DWH ''',
#         ''' TRUNCATE TABLE [stagging].[Asset] ''',
#         ''' INSERT INTO [stagging].[Asset]
#         (AssetID, ProjectID, Project, Technology, Cod, MW, SuccessPct, InstalledPower, Eoh, DateMerchant,
#         DismantleDate, Repowering, DateMsi, InPlanif, P50, P90)
#         VALUES (309, 'JB01', 'John Doe', 'éolien', '2023-02-01', 25.20, 1, 25.2, 
#         0.000, '2043-02-01', '1901-01-01', 'None', '2023-02-01', 'Non', 55188.000, 44150.400),
#         (174, 'GO16', 'Préveranges', 'éolien', '2023-03-01', 12.00, 1.00000, 12.00000, 0.000, '2043-03-01', 
#         '1901-01-01', 'None', '2023-03-01', 1, 26280.000, 21024.000) ''',
#         ''' MERGE [dwh].[DimAsset] AS DST
#             USING [stagging].[Asset] AS SRC
#             ON (SRC.AssetId = DST.AssetId AND SRC.ProjectId = DST.ProjectId)
#             WHEN NOT MATCHED THEN
#             INSERT (AssetId, ProjectId, Project, Technology, Cod, MW, SuccessPct, InstalledPower, Eoh, 
#             DateMerchant, DismantleDate, Repowering, DateMsi, InPlanif, P50, P90)
#             VALUES (SRC.AssetId, SRC.ProjectId, SRC.Project, SRC.Technology, SRC.Cod, SRC.MW, SRC.SuccesPct, SRC.InstalledPower, SRC.Eoh, 
#             SRC.DateMerchant, SRC.DismantleDate, SRC.Repowering, SRC.DateMsi, SRC.InPlanif, SRC.P50, SRC.P90)
#             WHEN MATCHED 
#             AND (
#             ISNULL(DST.Project,'') <> ISNULL(SRC.Project,'') 
#              OR ISNULL(DST.Technology,'') <> ISNULL(SRC.Technology,'') 
#              OR ISNULL(DST.Cod,'') <> ISNULL(SRC.Cod,'')
#              OR ISNULL(DST.MW,'') <> ISNULL(SRC.MW,'')
#              OR ISNULL(DST.SuccessPct,'') <> ISNULL(SRC.SuccessPct,'')
#              OR ISNULL(DST.InstalledPower,'') <> ISNULL(SRC.InstalledPower,'')
#              OR ISNULL(DST.Eoh,'') <> ISNULL(SRC.Eoh,'')
#              OR ISNULL(DST.DateMerchant,'') <> ISNULL(SRC.DateMerchant,'')
#              OR ISNULL(DST.DismantleDate,'') <> ISNULL(SRC.DismantleDate,'')
#              OR ISNULL(DST.Repowering,'') <> ISNULL(SRC.Repowering,'')
#              OR ISNULL(DST.DateMsi,'') <> ISNULL(SRC.DateMsi,'')
#              OR ISNULL(DST.InPlanif,'') <> ISNULL(SRC.InPlanif,'')
#              OR ISNULL(DST.P50,'') <> ISNULL(SRC.P50,'')
#              OR ISNULL(DST.P90,'') <> ISNULL(SRC.P90,'')

#              )
#             THEN UPDATE 
#             SET 
#              DST.Project = SRC.Project 
#              ,DST.Technology = SRC.Technology 
#              ,DST.Cod = SRC.Cod
#              ,DST.MW = SRC.MW
#              ,DST.SuccessPct = SRC.SuccessPct
#              ,DST.InstalledPower = SRC.InstalledPower
#              ,DST.Eoh = SRC.Eoh
#              ,DST.DateMerchant = SRC.DateMerchant 
#              ,DST.DismantleDate = SRC.DismantleDate
#              ,DST.Repowering = SRC.Repowering
#              ,DST.DateMsi = SRC.DateMsi
#              ,DST.InPlanif = SRC.InPlanif
#              ,DST.P50 = SRC.P50
#              ,DST.P90 = SRC.P90; '''
#     ], 
#     mssqlserver=mssqlserver, 
#     mssqldb=mssqldb)
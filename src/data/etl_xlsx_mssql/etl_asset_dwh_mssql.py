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

pgpw=os.path.join(os.path.dirname("__file__"),config['develop']['pgpw'])
pgport=os.path.join(os.path.dirname("__file__"),config['develop']['pgport'])
pguid=os.path.join(os.path.dirname("__file__"),config['develop']['pguid'])
pgserver=os.path.join(os.path.dirname("__file__"),config['develop']['pgserver'])
pgstgdb=os.path.join(os.path.dirname("__file__"),config['develop']['pgstgdb'])
pgdwhdb=os.path.join(os.path.dirname("__file__"),config['develop']['pgdwhdb'])
mssqlserver = os.path.join(os.path.dirname("__file__"),config['develop']['mssqlserver'])
mssqldb = os.path.join(os.path.dirname("__file__"),config['develop']['mssqldb'])

if __name__ == '__main__':
    excucute_postgres_crud_ops(
        queries=[ 
        '''INSERT INTO dwh."D_Asset" ("AssetId", "ProjectId", "Project", "Technology", "Cod", "MW", "SuccessPct", 
            "InstalledPower", "Eoh", "DateMerchant", "DismantleDate", "Repowering", "DateMsi", "InPlanif", "P50", 
            "P90", "LastUpdated", "creationdate")
        SELECT 
            SRC."AssetId", SRC."ProjectId", SRC."Project", SRC."Technology", SRC."Cod", SRC."MW", SRC."SuccessPct", 
            SRC."InstalledPower", SRC."Eoh", SRC."DateMerchant", SRC."DismantleDate", SRC."Repowering", SRC."DateMsi", 
            SRC."InPlanif", SRC."P50", SRC."P90", now()::timestamp, now()::timestamp 
        FROM stagging."Asset" AS SRC
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
            "P90" = EXCLUDED."P90",
            "LastUpdated" = EXCLUDED."LastUpdated";'''],  
        pguid=pguid, 
        pgpw=pgpw, 
        pgserver=pgserver,
        pgport=pgport,
        pgdb=pgdwhdb,
        params=None
        )

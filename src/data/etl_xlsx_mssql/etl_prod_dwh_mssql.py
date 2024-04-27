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
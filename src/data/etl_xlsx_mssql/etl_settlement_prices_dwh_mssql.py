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
        '''UPDATE "stagging"."MarketPrices" 
           SET "DateId" = to_char("DeliveryPeriod", 'YYYYMMDD')::integer;''',
        '''UPDATE "stagging"."MarketPrices" 
           SET "Year" = EXTRACT(YEAR FROM "DeliveryPeriod")::integer;''',
        '''UPDATE "stagging"."MarketPrices" 
           SET "Quarter" = EXTRACT(QUARTER FROM "DeliveryPeriod")::integer;''',
        '''UPDATE "stagging"."MarketPrices" 
           SET "Month" = EXTRACT(MONTH FROM "DeliveryPeriod")::integer;'''], 
                               pguid=pguid, 
                               pgpw=pgpw, 
                               pgserver=pgserver,
                               pgport=pgport,
                               pgdb=pgdwhdb, 
                               params=None)
    excucute_postgres_crud_ops(queries=[
        '''INSERT into dwh."I_MarketPrices" ( 
        "HedgeId", "DateId", "ProjectId", "DeliveryPeriod", "Year", "Quarter", "Month", "SettlementPrice"
        ) 
        select 
            sp."HedgeId", sp."DateId", sp."ProjectId", sp."DeliveryPeriod", sp."Year", sp."Quarter", sp."Month", sp."SettlementPrice"
            from stagging."MarketPrices" as sp;'''], 
                                   pguid=pguid, 
                                   pgpw=pgpw, 
                                   pgserver=pgserver,
                                   pgport=pgport,
                                   pgdb=pgdwhdb,
                                   params=None) 



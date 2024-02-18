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
        '''UPDATE "stagging"."VolumeHedge" 
           SET "DateId" = to_char("Date", 'YYYYMMDD')::integer;'''], 
                                   pguid=pguid, 
                                   pgpw=pgpw, 
                                   pgserver=pgserver,
                                   pgport=pgport,
                                   pgdb=pgdwhdb,
                                   params=None)
    excucute_postgres_crud_ops(queries=[
        '''INSERT into dwh."I_Hedge" (
        "HedgeId", "DateId", "ProjectId", "Project", "TypeHedge", "Date", "Year", "Quarter", "Month", "P50H", "P90H") 
        select 
            vh."HedgeId", vh."DateId", vh."ProjectId", vh."Project", vh."TypeHedge", vh."Date", vh."Year", vh."Quarter", vh."Month", vh."P50", vh."P90" 
            from stagging."VolumeHedge" as vh;'''], 
                                   pguid=pguid, 
                                   pgpw=pgpw, 
                                   pgserver=pgserver,
                                   pgport=pgport,
                                   pgdb=pgdwhdb,
                                   params=None)
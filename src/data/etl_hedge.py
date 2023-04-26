import pandas as pd
import numpy as np
from datetime import datetime
import sys
import configparser
import os
pd.options.mode.chained_assignment=None
os.chdir('D:/local-repo-github/enr_portfolio_modeling/')
from src.utils.functions import*
from etl import*

config_file=os.path.join(os.path.dirname("__file__"), 'Config/config.ini') 
config=configparser.ConfigParser(allow_no_value=True)
config.read(config_file)

hedge_vmr = os.path.join(os.path.dirname("__file__"),config['develop']['hedge_vmr'])
hedge_planif = os.path.join(os.path.dirname("__file__"),config['develop']['hedge_planif'])
src_dir = os.path.join(os.path.dirname("__file__"),config['develop']['src_dir'])
dest_dir = os.path.join(os.path.dirname("__file__"),config['develop']['dest_dir'])
temp_dir = os.path.join(os.path.dirname("__file__"),config['develop']['temp_dir'])
google_application_credentials = os.path.join(os.path.dirname("__file__"),config['develop']['google_application_credentials'])
datasetid = os.path.join(os.path.dirname("__file__"),config['develop']['datasetid']) #gbq stg ddb
tableid = os.path.join(os.path.dirname("__file__"),config['develop']['tableid'])
bucketid = os.path.join(os.path.dirname("__file__"),config['develop']['bucketid'])
mssqluid = os.path.join(os.path.dirname("__file__"),config['develop']['mssqluid'])
mssqlserver = os.path.join(os.path.dirname("__file__"),config['develop']['mssqlserver'])
msqsldriver = os.path.join(os.path.dirname("__file__"),config['develop']['mssqlserver'])
mssqldb = os.path.join(os.path.dirname("__file__"),config['develop']['mssqldw'])
uri = os.path.join(os.path.dirname("__file__"),config['develop']['uri'])
mongodbatlas_stg_conn_str = os.path.join(os.path.dirname("__file__"),config['develop']['mongodbatlas_stg_conn_str']) 


if __name__ == '__main__':
    df_hedge_vmr, df_hedge_planif = extract_hedge(hedge_vmr_path=hedge_vmr, hedge_planif_path=hedge_planif)
    src_data = transform_hedge(hedge_vmr=df_hedge_vmr, hedge_planif=df_hedge_planif)
    load_hedge(dest_dir=dest_dir, src_flow=src_data, file_name="template_hedge", file_extension='.csv')
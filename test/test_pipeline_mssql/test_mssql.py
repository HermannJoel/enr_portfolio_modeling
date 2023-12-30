import pytest
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
config_file=os.path.join(os.path.dirname('__file__'), 'Config/config.ini') 
config=configparser.ConfigParser(allow_no_value=True)
config.read(config_file)

pgpw=os.path.join(os.path.dirname("__file__"),config['develop']['pgpw'])
pguid=os.path.join(os.path.dirname("__file__"),config['develop']['pguid'])
pgserver=os.path.join(os.path.dirname("__file__"),config['develop']['pgserver'])
pgdwhdb=os.path.join(os.path.dirname("__file__"),config['develop']['pgdwhdb'])
pgport=os.path.join(os.path.dirname("__file__"),config['develop']['pgport'])


results = execute_data_queries_from_postgresql(queries={'contract_prices':'select count(*) from dwh."I_ContractPrices" cp where cp."ProjectId"=\'S18\' and cp."ContractPrice" is not null;',
                                                           'volume_hedge':'select count(*) from dwh."I_Hedge" h where h."ProjectId"=\'S18\' and h."P50H" is not null;'
                                                          }, 
                                                  pguid=pguid, 
                                                  pgpw=pgpw, 
                                                  pgserver=pgserver, 
                                                  pgport=pgport, 
                                                  pgdb=pgdwhdb)

@pytest.fixture(scope="session", autouse=True)
def queries_results():
    results = execute_data_queries_from_postgresql(queries={'contract_prices':'select count(*) from dwh."I_ContractPrices" cp where cp."ProjectId"=\'S18\' and cp."ContractPrice" is not null;',
                                                           'volume_hedge':'select count(*) from dwh."I_Hedge" h where h."ProjectId"=\'S18\' and h."P50H" is not null;',
                                                           'sta_production':'select round(sum(pa."P50")/1000, 2) as production from stagging."ProductionAsset" as pa \
                                                           where pa."Year"=2022 and pa."ProjectId" in (select a."ProjectId" from stagging."Asset" a where a."InPlanif"=False);',
                                                           'sta_productibles':'select round(sum(a."P50")/1000,2) as productibles from stagging."Asset" a where a."InPlanif" = false;'
                                                           'dwh_production':'select round(sum(ia."P50A")/1000, 2) as production from dwh."I_Asset" ia \
                                                           where ia."Year"=2022 and ia."ProjectId" in (select da."ProjectId" from dwh."D_Asset" da where da."InPlanif"=False);',
                                                           'dwh_productibles':'select round(sum(da."P50")/1000,2) as productibles from dwh."D_Asset" da where da."InPlanif" = false;'
                                                          }, 
                                                  pguid=pguid, 
                                                  pgpw=pgpw, 
                                                  pgserver=pgserver, 
                                                  pgport=pgport, 
                                                  pgdb=pgdwhdb)
    return results
    
def test_queries_results(queries_results):
    assert queries_results['contract_prices'].loc[0, 'count'] == queries_results['volume_hedge'].loc[0, 'count'] 
    assert queries_results['sta_production'].loc[0, 'production'] == queries_results['sta_productibles'].loc[0, 'productibles']
    assert queries_results['dwh_production'].loc[0, 'production'] == queries_results['dwh_productibles'].loc[0, 'productibles']
    assert queries_results['sta_production'].loc[0, 'production'] == queries_results['dwh_production'].loc[0, 'production']
    assert queries_results['sta_productibles'].loc[0, 'productibles'] == queries_results['dwh_productibles'].loc[0, 'productibles']
    




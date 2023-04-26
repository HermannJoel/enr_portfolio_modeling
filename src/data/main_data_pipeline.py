import sys
import os
import configparser
ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath('__file__'))) # This is your Project Root
os.chdir('D:/local-repo-github/enr_portfolio_modeling/')
from functions import* 
from etl import market_prices_curve, extract, market_prices_curve_estimation, load_docs_to_mongodb
#Load Config
config_file=os.path.join(os.path.dirname("__file__"), 'Config/config.ini') 
config=configparser.ConfigParser(allow_no_value=True)
config.read(config_file)

wm = os.path.join(os.path.dirname("__file__"),config['develop']['wm'])
wq = os.path.join(os.path.dirname("__file__"),config['develop']['wq'])
fp = os.path.join(os.path.dirname("__file__"),config['develop']['future_products'])
mongodbatlas_stg_conn_str = os.path.join(os.path.dirname("__file__"),config['develop']['mongodbatlas_stg_conn_str']) 


#To load settlement prices from eex to mongodb stg db EEX 
if __name__ == '__main__':
    mb, qb, calb, q_w, m_w = extract(future_prices_path = fp,
                                     q_w_path = wq, 
                                     m_w_path = wm)
    load_docs_to_mongodb(dest_db='staging', dest_collection='EEX', 
                         src_data=market_prices_curve_estimation(mb = mb, qb = qb, yb = calb, q_weights = q_w, m_weights = m_w), 
                         date_format = '%Y-%m-%d', mongodb_conn_str = mongodbatlas_stg_conn_str)

#To load hedge
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

hedge_vmr = os.path.join(os.path.dirname("__file__"),config['develop']['hedge_vmr'])
hedge_planif = os.path.join(os.path.dirname("__file__"),config['develop']['hedge_planif'])
src_dir = os.path.join(os.path.dirname("__file__"),config['develop']['src_dir'])
dest_dir = os.path.join(os.path.dirname("__file__"),config['develop']['dest_dir'])
temp_dir = os.path.join(os.path.dirname("__file__"),config['develop']['temp_dir'])
future_products = os.path.join(os.path.dirname("__file__"),config['develop']['future_products'])
val_dir = os.path.join(os.path.dirname("__file__"),config['develop']['ge_val_dir'])
wq = os.path.join(os.path.dirname("__file__"),config['develop']['wq'])
wm = os.path.join(os.path.dirname("__file__"),config['develop']['wm'])

if __name__ == '__main__':
    #do_scrap_eex(i=)
    calb, qb, mb, q_w, m_w = extract_settlement_prices_data(future_prices_path = future_products, q_w_path = wq, m_w_path = wm)
    prices_mb = settlement_prices_curve_estimation(yb = calb, qb = qb, mb = mb, q_weights = q_w, m_weights = m_w, horizon_q = horizon_q, horizon_m = horizon_m)
    load_settlement_prices_as_excel(dest_dir = val_dir, src_flow = prices_mb, file_name = 'settl_prices', file_extension = '.csv')    

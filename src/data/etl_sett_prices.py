import sys
import configparser
import os
os.chdir('D:/local-repo-github/enr_portfolio_modeling/')
from src.utils.functions import*
from etl import*

#Load Config
config_file=os.path.join(os.path.dirname("__file__"), 'Config/config.ini') 
config=configparser.ConfigParser(allow_no_value=True)
config.read(config_file)

if __name__ == '__main__':
    do_scrap_eex(i)
    mb, qb, calb, q_w, m_w = extract_settlement_price(future_prices_path, q_w_path, m_w_path)
    prices_mb = settlement_prices_curve_estimation(mb, qb, yb, q_weights, m_weights, horizon_q = horizon_q, horizon_m = horizon_m)
    load_settlement_prices_as_excel(dest_dir, src_flow = prices_mb, file_name, file_extension)
    
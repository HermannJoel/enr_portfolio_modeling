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

src_dir = os.path.join(os.path.dirname("__file__"),config['develop']['src_dir'])
dest_dir = os.path.join(os.path.dirname("__file__"),config['develop']['processed_files_dir'])
temp_dir = os.path.join(os.path.dirname("__file__"),config['develop']['temp_dir'])
future_products = os.path.join(os.path.dirname("__file__"),config['develop']['future_products'])
wq = os.path.join(os.path.dirname("__file__"),config['develop']['wq'])
wm = os.path.join(os.path.dirname("__file__"),config['develop']['wm'])

nb_years=(2029-dt.today().year)#2008 represents end year of time horizon. Change the year to the year that suits the desired horizon
nb_months=12#Number of months in one year
nb_quarters=nb_years*4#To compute the number of quarter in our time horizon
nb_eex_qb_cotation=5 #Nber of quarterly product cotation available in eex website.
horizon_m=(nb_months*nb_years)-2#To remove the month of July/Aug. We are now in Sep. To determine the number of month in the time horizon
horizon_q=nb_quarters-nb_eex_qb_cotation#To determine the number of quarter in the time horizon for which we have to compute prices

if __name__ == '__main__':
    #do_scrap_eex(i=)
    calb, qb, mb, q_w, m_w = extract_settlement_prices_data(future_prices_path = future_products, q_w_path = wq, m_w_path = wm)
    prices_mb = settlement_prices_curve_estimation(yb = calb, qb = qb, mb = mb, q_weights = q_w, m_weights = m_w, horizon_q = horizon_q, horizon_m = horizon_m)
    load_settlement_prices_as_excel(dest_dir=dest_dir , src_flow = prices_mb, file_name = 'settlement_prices', file_extension = '.csv')

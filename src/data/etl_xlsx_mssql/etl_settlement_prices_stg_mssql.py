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
mongodbatlas_dw_conn_str = os.path.join(os.path.dirname("__file__"),config['develop']['mongodbatlas_dw_conn_str'])
pgpw=os.path.join(os.path.dirname("__file__"),config['develop']['pgpw'])
pguid=os.path.join(os.path.dirname("__file__"),config['develop']['pguid'])
pgserver=os.path.join(os.path.dirname("__file__"),config['develop']['pgserver'])
pgdwhdb=os.path.join(os.path.dirname("__file__"),config['develop']['pgdwhdb'])
pgport=os.path.join(os.path.dirname("__file__"),config['develop']['pgport'])

nb_years=(2029-dt.today().year)#2028 represents end year of time horizon. Change the year to the year that suits the desired horizon
nb_months=12#Number of months in one year
nb_quarters=nb_years*4#To compute the number of quarter in our time horizon
nb_eex_qb_cotation=5 #Nber of quarterly product cotation available in eex website.
horizon_m=(nb_months*nb_years)-2#To remove the month of July/Aug. We are now in Sep. To determine the number of month in the time horizon
horizon_q=nb_quarters-nb_eex_qb_cotation#To determine the number of quarter in the time horizon for which we have to compute prices

if __name__ == '__main__':
    #do_scrap_eex(i=)
    calb, qb, mb, q_w, m_w = extract_settlement_prices_data(future_prices_path = future_products, q_w_path = wq, m_w_path = wm)
    prices_mb=settlement_prices_curve_estimation(yb = calb, qb = qb, mb = mb, q_weights = q_w, m_weights = m_w, horizon_q = horizon_q, horizon_m = horizon_m)   
    load_settlement_prices_as_excel(dest_dir = dest_dir, src_flow = prices_mb, file_name = 'settlement_prices', file_extension = '.csv')   
    # load_docs_to_mongodb(dest_db='dw', dest_collection='SettlementPricesCurve', 
    #                      src_data= data_prices_mb, 
    #                      date_format = '%Y-%m-%d', 
    #                      mongodb_conn_str = mongodbatlas_dw_conn_str)
    df_prices_curve = read_docs_from_mongodb(src_db = 'dw', src_collection = 'SettlementPricesCurve', 
                                             query={'CotationDate':datetime.strptime('2022-09-07', '%Y-%m-%d')}, no_id=True, 
                                             column_names=['DeliveryPeriod', 'SettlementPrice', 
                                                    'CotationDate'], 
                                             mongodb_conn_str = mongodbatlas_dw_conn_str)
    df_hedge=read_docs_from_mongodb(src_db='dw', 
                                    src_collection='Hedge',  
                                    query={}, 
                                    no_id=True,
                                    column_names=["Id", "HedgeId", "AssetId", "ProjectId", "Project", "Technology", "TypeHedge", "ContractStartDate", 
                                                  "ContractEndDate", "DismantleDate", "InstalledPower", "InPlanif", "Profil", "HedgePct", 
                                                  "Countreparty", "CountryCountreparty"], 
                                    mongodb_conn_str=mongodbatlas_dw_conn_str)
    settlement_prices=model_settlement_prices(data_template_hedge=df_hedge, data_settlement_prices=df_prices_curve)
    settlement_prices=sqldf("""select sp."HedgeId", sp."ProjectId", sp."DeliveryPeriod", sp."SettlementPrice" 
                      from settlement_prices sp;""", locals())
    
    # excucute_postgres_crud_ops(
    #     queries=[
    #     '''INSERT INTO dwh."I_MtM" ("DateId", "CotationDate", "MtM")
    #     VALUES (0, '2022-09-01', -356.03 ),
    #           (0, '2022-09-02', -549.38000),
    #           (0, '2022-09-05', -581.69000),
    #           (0, '2022-09-06', -520.63000),
    #           (0, '2022-09-07', -478.22000),
    #           (0, '2022-09-08', -464.96000),
    #           (0, '2022-09-09', -461.39000),
    #           (0, '2022-09-12', -442.09000),
    #           (0, '2022-09-13', -451.23000),
    #           (0, '2022-09-14', -494.33000),
    #           (0, '2022-09-15', -523.15000),
    #           (0, '2022-09-16', -502.61000);'''],  
    #     pguid=pguid, 
    #     pgpw=pgpw, 
    #     pgserver=pgserver,
    #     pgport=pgport,
    #     pgdb=pgdwhdb,
    #     params=None
    #     )
    # excucute_postgres_crud_ops(
    #     queries=[
    #     '''UPDATE "dwh"."I_MtM" 
    #        SET "DateId" = to_char("CotationDate", 'YYYYMMDD')::integer;'''],  
    #     pguid=pguid, 
    #     pgpw=pgpw, 
    #     pgserver=pgserver,
    #     pgport=pgport,
    #     pgdb=pgdwhdb,
    #     params=None
    #     )
    
    excucute_postgres_crud_ops(
        queries=[
        '''TRUNCATE TABLE stagging."MarketPrices";'''],  
        pguid=pguid, 
        pgpw=pgpw, 
        pgserver=pgserver,
        pgport=pgport,
        pgdb=pgdwhdb,
        params=None
        )
    settlement_prices["SettlementPrice"] = settlement_prices["SettlementPrice"].apply(pd.to_numeric, errors='coerce')
    load_data_in_postgres_table(src_data=settlement_prices, dest_table='MarketPrices', 
                                pguid=pguid, pgpw=pgpw, pgserver=pgserver,  
                                pgdb=pgdwhdb, schema='stagging', if_exists='append')
    




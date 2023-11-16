import pandas as pd
import numpy as np

#This is to get prod data namely p_50, p_90  for template asset table
#To import projet_id, p_50, p_90 
prod = pd.read_excel(path_dir_in + "template_prod.xlsx", sheet_name="prod", usecols=['projet_id', 'p50', 'p90'])
prod_planif_sol = pd.read_excel(path_dir_temp + "prod_planif_solaire.xlsx", usecols=['projet_id', 'p50', 'p90'])
prod_planif_eol = pd.read_excel(path_dir_temp + "prod_planif_eolien.xlsx", usecols=['projet_id', 'p50', 'p90'])


#To combine p50 & p90 planif_eol, planif_sol
frames = [prod, prod_planif_sol, prod_planif_eol]
prod_asset = pd.concat(frames)
prod_asset.reset_index(inplace=True, drop=True)

#To export prod data for asset tamplate 
prod_asset.to_excel(path_dir_temp + 'prod_asset.xlsx', index=False, float_format="%.3f")


#To import p50_P90_asset, p50_P90_hedge and contract prices(strike prices OA, CR, PPA)   
#To rename columns to merge the 3 data frames
prod_asset=pd.read_excel(path_dir_in+'p50_P90_asset_vmr_planif.xlsx')
prod_asset.rename(columns={'rw_id':'rw_id_a', 'projet':'projet_a', 
                           #'année':'année_a', 'trim':'trim_a', 'mois':'mois_a', 
                           'p50_adj':'p50_adj_a', 'p90_adj':'p90_adj_a'}, inplace=True)

prod_hedge=pd.read_excel(path_dir_in+'p50_P90_hedge_vmr_planif.xlsx')
prod_hedge.rename(columns={'rw_id':'rw_id_h', 'projet':'projet_h', 
                           #'année':'année_h', 'trim':'trim_h', 'mois':'mois_h', 
                           'p50_adj':'p50_adj_h', 'p90_adj':'p90_adj_h'}, inplace=True)

prices=pd.read_excel(path_dir_in+'contracts_prices_oa_cr_ppa.xlsx')
prices.rename(columns={'rw_id':'rw_id_p','projet':'projet_p', 
                       #'année':'année_p', 'trim':'trim', 'mois':'mois_p'
                      }, inplace=True)
print(prod_asset.shape)
print(prod_hedge.shape)
print(prices.shape)

#To merge p_50, P_90 asset and p_50, P90_hedge
#prod_ah=pd.merge(prod_asset, prod_hedge, how='left', on=['projet_id', 'année', 'trim', 'mois'])
#print(prod_ah.shape)

#To merge p_50, p_50, P90_hedge and contract prices(OA, CR, PPA)
prod_h_prices=pd.merge(prod_hedge, prices, how='inner', on=['projet_id', 'hedge_id', 'date', 'année', 'mois'])
print(prod_h_prices.shape)

df_temp_1=prod_ah_prices.loc[(prod_ah_prices['projet_id']=='ECO8') & (prod_ah_prices['hedge_id']==120)]
df_temp_1.tail(50)

#### This notebook is to combine hedge data as `projet_id`, `cod`, `date_merchant`, `date_dementelement` and market prices
#Pull the monthly market prices scrapped from eex and save in the table `DIM_settlement_prices_fr_eex`.
#Derrived the prices curves accross our time frame 2022-2028.
#Merge the monthly market prices with hedge_id
#Export as hedge_settlement_prices


asset_d=pd.read_excel(path_dir_in+"template_asset.xlsx")
hedge_d=pd.read_excel(path_dir_in+"template_hedge.xlsx")

query_result=pd.read_sql_query('''
                                SELECT
                                       delivery_period,
                                       settlement_price,
                                       last_update,
                                       DATEPART(YEAR, delivery_period) AS years,
                                       DATEPART(QUARTER, delivery_period) AS quarters,
                                       DATEPART(MONTH, delivery_period) AS months
                                       FROM DIM_settlement_prices_fr_eex 
                                  WHERE current_v=1 AND last_update='2022-08-26';
                                ''', cnx)

market_prices=query_result[['delivery_period', 'settlement_price', 'years', 'quarters', 'months']]
market_prices.head()

asset_d_=asset_d[['projet_id', 'cod', 'date_merchant', 'date_dementelement']]
hedge_d_=hedge_d[['hedge_id', 'projet_id', 'date_debut', 'date_fin']]

#To multiply hedge df by the len of prices df
n=len(market_prices)
df_hedge = pd.DataFrame(
                np.repeat(hedge_d_.values, n, axis=0),
                columns=hedge_d_.columns,
            )

#To multiply prices df by the len of hedge df
n=len(hedge_d_)
market_prices_=pd.concat([market_prices]*n, ignore_index=True)

frame=[df_hedge, market_prices_]
hedge_market_prices=pd.concat(frame, axis=1, ignore_index=False)

#To multiply hedge df by the len of prices df
n=len(df_settlement_prices)
df_hedge = pd.DataFrame(
                np.repeat(df_template_hedge.values, n, axis=0),
                columns=df_template_hedge.columns,
            )

#To multiply prices df by the len of hedge df
n=len(df_template_hedge)
df_settl_prices=pd.concat([df_settlement_prices]*n, ignore_index=True)

frame=[df_hedge, df_settl_prices]
hedge_settl_prices=pd.concat(frame, axis=1, ignore_index=False)
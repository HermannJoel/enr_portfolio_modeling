import pandas as pd
import numpy as np
xrange = range
import os
import configparser
from datetime import datetime
import sys
pd.options.mode.chained_assignment = None
sys.path.append('/mnt/d/local-repo-github/enr_portfolio_modeling')
os.chdir('/mnt/d/local-repo-github/enr_portfolio_modeling/')
from src.utils.functions import*
from etl import*

def transform_prices(data_prices, sub_template_asset, **kwargs):
    """
    udf Function to generate template contracts prices asset in prod
    Parameters
    ===========
    **kwargs
        hedge_vmr: DataFrame
                
        hedge_planif: DataFrame
    prices: DataFrame
        data frame contract prices
    template_asset: DataFrame
    Returns
    =======
    template_prices: DataFrame
        template prices dataframe
    """
    try:
        prices=data_prices.iloc[:106, 80:93]
        #To rename columns
        prices.rename(columns={'Site.4': 'site', 'JAN [€/MWh].3': 'jan', 'FEB [€/MWh].3':'feb', 
                              'MAR [€/MWh].3':'mar', 'APR [€/MWh].3':'apr', 'MAY [€/MWh].3':'may', 
                              'JUNE [€/MWh].3':'june', 'JULY [€/MWh].3':'july', 'AUG [€/MWh].3':'aug', 
                              'SEPT [€/MWh].3':'sep', 'OCT [€/MWh].3':'oct', 'NOV [€/MWh].3':'nov', 
                              'DEC [€/MWh].3':'dec'}, inplace=True)
        
        #To create a list containing projets out of service
        out_projets = ['Blendecques Elec', 'Bougainville', 'Cham Longe Bel Air', 'CDB Doux le vent' ,
                       'Cham Longe Le Courbil (Eole Cevennes)', 'Evits et Josaphats', 'La Bouleste', 
                       'Renardières mont de Bezard', 'Remise Reclainville', "Stockage de l'Arce", ]

        #To change PBF Blanches Fosses into Blanches Fosses PBF
        prices.loc[prices['site']=='PBF Blanches Fosses', 'site']='Blanches Fosses PBF'
        #To drop rows that contain any value in the list and reset index
        prices = prices[prices['site'].isin(out_projets) == False]
        prices.sort_values(by=['site'], inplace=True, ignore_index=True)
        prices.reset_index(inplace=True, drop=True)

        #To import projet_id from template asset
        projet_names_id = sub_template_asset
        projet_names_id = projet_names_id.loc[projet_names_id["en_planif"] == False]
        projet_names_id.sort_values(by=['projet'], inplace=True, ignore_index=True)
        projet_names_id.drop("en_planif", axis=1, inplace=True)
        projet_names_id.reset_index(drop=True, inplace=True)
        #rename projet_id as code
        projet_names_id.rename(columns={"projet_id":"code"}, inplace=True)
        
        #prices_id=assign_value_to_column(n=30, df=prices, df_=projet_names_id, target_col_df='projet_id', args_1_df_='projet', args_2_df_='code', args_1_df='site')
        #To join projet_id to template_price
        frame=[projet_names_id, prices]
        prices_id = pd.concat(frame, axis=1, ignore_index=False)
        #To create a new column with projet_id
        #Compare the 1st 5 character of projet names and site. set projet_id=code when the values match.
        n = 7
        #prices_id.loc[prices_id['site'].str[:n] == prices_id['projet'].str[:n], 'projet_id'] = prices_id["code"]
        prices_id['projet_id'] = prices_id.apply(lambda row: row['code'] if row['projet'][:n] == row['site'][:n] else None, axis=1)
        template_prices=prices_id[["projet_id", "site", "jan", "feb", "mar", "apr", "may", "june","july", 
                                   "aug", "sep", "oct", "nov", "dec", ]]
        
        return template_prices
    
    except Exception as e:
        print("Template prices transformation error!: "+str(e))



        
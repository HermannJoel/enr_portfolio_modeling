import pandas as pd
import numpy as np
xrange = range
import os
import configparser
from datetime import datetime
import sys
os.chdir('D:/local-repo-github/enr_portfolio_modeling/')
from functions import* 

#Load Config
config_file=os.path.join(os.path.dirname("__file__"), 'Config/config.ini') 
config=configparser.ConfigParser(allow_no_value=True)
config.read(config_file)

# Initialize Variables
dest_dir=os.path.join(os.path.dirname("__file__"),config['develop']['dest_dir'])
dest_dir=os.path.join(os.path.dirname("__file__"),config['develop']['dest_dir'])
template_hedge=os.path.join(os.path.dirname("__file__"),config['develop']['template_hedge'])
template_prices=os.path.join(os.path.dirname("__file__"),config['develop']['templates_prices'])

class etl_contract_prices(object):
    """Class to Extract Transform and Load contract prices data.
    Attributes
    ==========
    template_hedge_path : DataFrame
        template hedge
    df_hedge : DataFrame
    
    Methods
    =======
    extract_data
        return templates hedge & prices data
    transform_prices_planif
        transform prices of asset in planif 
    load_prices_planif
        load transformed data in excel file
    """
    def __init__(self, template_hedge_path, df_hedge_=0):
        self.template_hedge_path=template_hedge_path
        self.df_hedge_=df_hedge_
    
    def extract_data(self):
        try:
            df_hedge_=ReadExcelFile(self.template_hedge_path)
            return df_hedge_
        
        except Exception as e:
            print("Data Extration error!: "+str(e))
        
    def transform_prices_planif(self, df_hedge=None):
        try:
            if df_hedge is None:
                df_hedge = self.df_hedge_
                df_hedge=df_hedge.loc[df_hedge['en_planif']=='Oui']
                df_hedge.reset_index(drop=True, inplace=True)
                #create a list containing assets under ppa contracts
                ppa=['Ally Bessadous', 'Ally Mercoeur', 'Ally Monteil', 
                     'Ally Verseilles', 'Chépy', 'La citadelle', 'Nibas', 
                     'Plouguin', 'Mazagran', 'Pézènes-les-Mines']
                #To create subset of solar and wind power
                df_hedge_wp=df_hedge.loc[(df_hedge['technologie']=='éolien')]
                df_hedge_sol=df_hedge.loc[(df_hedge['technologie']=='solaire')]
                #To remove ppa from solar and wind power
                df_hedge_sol=df_hedge_sol[df_hedge_sol['projet'].isin(ppa) == False]
                df_hedge_wp=df_hedge_wp[df_hedge_wp['projet'].isin(ppa) == False]

                df_hedge_sol=df_hedge_sol.iloc[:,np.r_[1, 2, 3, 5, 6, 7]]
                n_sol=len(df_hedge_sol)
                df_hedge_wp=df_hedge_wp.iloc[:,np.r_[1, 2, 3, 5, 6, 7]]
                n_wp=len(df_hedge_wp)
                print('create solar & wind power dfs:\n')
                #create a df solar
                d1=CreateMiniDataFrame(df_hedge_sol, '01-01-2022', n=n_sol, a=0, b=12*7, date='date')   
                d1.reset_index(drop=True, inplace=True)
                #create a df wind power
                d2=CreateMiniDataFrame(df_hedge_wp, '01-01-2022', n=n_wp, a=0, b=12*7, date='date')   
                d2.reset_index(drop=True, inplace=True)
                #To create quarter and month columns
                d1['année'] = d1['date'].dt.year
                d1['trimestre'] = d1['date'].dt.quarter
                d1['mois'] = d1['date'].dt.month
                d2['année'] = d2['date'].dt.year 
                d2['trimestre'] = d2['date'].dt.quarter
                d2['mois'] = d2['date'].dt.month
                #Create price column
                d1.loc[d1['type_hedge']=='CR', 'price'] = 60
                d2.loc[d2['type_hedge']=='CR', 'price'] = 70
                #To merge hedge_vmr and hedge_planif
                d=MergeDataFrame(d1, d2)
                #To remove price based on date_debut and date_fin
                prices_planif=RemoveContractPrices(data=d, sd='date_debut', ed='date_fin', price='price',
                                                   th='type_hedge', date='date', projetid='projet_id', 
                                                   hedgeid='hedge_id')

                prices_planif=SelectColumns(prices_planif, 'hedge_id', 'projet_id', 'projet', 'type_hedge', 'date_debut', 
                                            'date_fin', 'date', 'année', 'trimestre', 'mois', 'price') 

                return prices_planif
        except Exception as e:
            print("Data transformation error!: "+str(e))

#if __name__ == '__main__':
    #etl_contract_prices=etl_contract_prices(template_hedge)
    #etl_contract_prices.transform_prices_planif(etl_contract_prices.df_hedge_ = etl_contract_prices.extract_data())


# def Load(self, target_pth, src_data):
# src_data.to_csv(target_pth, ignore_index=True)
# loaded_data=Load(target_pth='D:/blx_mdp/cwd/in/prices_planif.txt', src_data=TransformPricesPlanif())
# #This method transform prices of assets in planification
# def TransformPricesProd(self, data, **kwargs):
# return prices_prod
      
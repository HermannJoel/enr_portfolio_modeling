import pandas as pd
import numpy as np
xrange = range
import os
import configparser
from datetime import datetime
import sys
pd.options.mode.chained_assignment = None
os.chdir('D:/local-repo-github/enr_portfolio_modeling/')
from src.utils.functions import*

def transform_contract_prices_planif(data_hedge):
    try:
        df_hedge = data_hedge
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
        d1=create_mini_data_frame(df_hedge_sol, '01-01-2022', n=n_sol, a=0, b=12*7, date='date')   
        d1.reset_index(drop=True, inplace=True)
        #create a df wind power
        d2=create_mini_data_frame(df_hedge_wp, '01-01-2022', n=n_wp, a=0, b=12*7, date='date')   
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
        d=merge_data_frame(d1, d2)
        #To remove price based on date_debut and date_fin
        prices_planif=remove_contract_prices(data=d, sd='date_debut', ed='date_fin', price='price',
                                             th='type_hedge', date='date', projetid='projet_id', 
                                             hedgeid='hedge_id')

        prices_planif=select_columns(prices_planif, 'hedge_id', 'projet_id', 'projet', 'type_hedge', 'date_debut', 
                                     'date_fin', 'date', 'année', 'trimestre', 'mois', 'price') 

        return prices_planif
    except Exception as e:
            print("Data transformation error!: "+str(e))

            
def transform_contract_price_ppa(template_asset, data_ppa, **kwargs):
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
        ppa_= data_ppa
        ppa_=ppa_.iloc[:,np.r_[0, 1, 2, 4, 5, 6, -1]]
        #Import date cod & date_dementelement from asset
        asset_ = template_asset
        asset_=asset_[['asset_id', 'projet_id', 'cod', 'date_dementelement', 'date_merchant']]
        asset_.reset_index(drop=True, inplace=True)
        #To merge ppa prices data and template asset 
        ppa=pd.merge(ppa_, asset_, how='left', on=['projet_id'])
        n_ppa = len(ppa)
        #To create a df containing all the dates within the time horizon  
        #df = ppa.copy()     
        #start_date = pd.to_datetime([date_obj] * nbr)
        #d = pd.DataFrame()
        #for i in range(0, horizon):
            #df_buffer= df 
            #df_buffer["date"] = start_date
            #d = pd.concat([d, df_buffer], axis=0)
            #start_date= start_date + pd.DateOffset(months=1)
        #reset index    
        #d.reset_index(drop=True, inplace=True)
        
        d = create_mini_data_frame(ppa, '01-01-2022', n=n_ppa, a=0, b=12*7, date='date')
        d.reset_index(inplace=True, drop=True)
        #To create quarter and month columns
        d['année'] = d['date'].dt.year
        d['trimestre'] = d['date'].dt.quarter
        d['mois'] = d['date'].dt.month

        #To remove price based on date_debut
        #Condition:date column is less (in total seconds) than first date for each projet_id's first date_cod value
        #cond=((d['date'] - d.groupby(['hedge_id', 'projet_id'])['date_debut'].transform('first')).dt.total_seconds())<0
        #d['price'] = np.where(cond,'', d['price'])
        #To remove price based on date_fin
        #cond_2=((d['date'] - d.groupby(['hedge_id', 'projet_id'])['date_fin'].transform('first')).dt.total_seconds())>0
        #d['price'] = np.where(cond_2, '', d['price'])
        #To remove price based on date_dementelemnt
        #cond_2=((d['date'] - d.groupby(['hedge_id', 'projet_id'])['date_dementelement'].transform('first')).dt.total_seconds())>0
        #d['price'] = np.where(cond_2, '', d['price'])
        
        prices_ppa = remove_contract_prices(data=d, sd='date_debut', ed='date_fin', price='price', 
                                            th='type_hedge', date_dementelement='date_dementelement', 
                                            date='date', projetid='projet_id', hedgeid='hedge_id')
        prices_ppa=select_columns(d,'hedge_id', 'projet_id', 'projet', 'type_hedge', 'date_debut', 
                                 'date_fin', 'date', 'année', 'trimestre', 'mois', 'price')
        return prices_ppa
        
    except Exception as e:
        print("Data transformation eror!:" +str(e))
        
def transform_contract_prices_inprod(template_asset, template_hedge, template_prices, data_ppa, **kwargs):
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
    date (str) :
    Returns
    =======
    template_prices: DataFrame
        template prices dataframe
    """
    try:
        hedge_ = template_hedge
        hedge=hedge_.loc[hedge_['en_planif'] == 'Non']
        hedge.reset_index(drop=True, inplace=True)
        #List containing ppa
        ppa=['Ally Bessadous', 'Ally Mercoeur', 'Ally Monteil', 'Ally Verseilles', 'Chépy', 'La citadelle', 
             'Nibas', 'Plouguin', 'Mazagran', 'Pézènes-les-Mines']

        hedge_planif_eol=hedge_.loc[(hedge_['en_planif'] == 'Oui') & (hedge_['technologie'] == 'éolien')]
        hedge_planif_sol=hedge_.loc[(hedge_['en_planif'] == 'Oui') & (hedge_['technologie'] == 'solaire')]
        #To create a subset of df containing only ppa
        hedge_ppa=hedge_planif_sol[hedge_planif_sol['projet'].isin(ppa) == True]

        #To remove ppa from solar and wind power
        hedge_planif_sol=hedge_planif_sol[hedge_planif_sol['projet'].isin(ppa) == False]
        hedge_planif_eol=hedge_planif_eol[hedge_planif_eol['projet'].isin(ppa) == False]

        hedge_planif_sol=hedge_planif_sol.iloc[:,np.r_[1, 2, 3, 5, 6, 7]]
        hedge_planif_eol=hedge_planif_eol.iloc[:,np.r_[1, 2, 3, 5, 6, 7]]
        hedge_ppa=hedge_ppa.iloc[:,np.r_[1, 2, 3, 5, 6, 7]]


        #asset_=pd.read_excel(path_dir_in+"template_asset.xlsx")
        asset=asset_.loc[asset_['en_planif']=='Non']
        asset=asset[['asset_id', 'projet_id', 'cod', 'date_merchant']]
        asset.reset_index(drop=True, inplace=True)
        #subseting
        asset_planif_sol = asset_.loc[(asset_['en_planif'] == 'Oui') & (asset_['technologie'] == 'solaire')]
        asset_planif_eol = asset_.loc[(asset_['en_planif'] == 'Oui') & (asset_['technologie'] == 'éolien')]
        #list containing ppa
        ppa=['Ally Bessadous', 'Ally Mercoeur', 'Ally Monteil', 'Ally Verseilles', 'Chépy', 'La citadelle', 
             'Nibas', 'Plouguin', 'Mazagran', 'Pézènes-les-Mines']

        #To filter asset under ppa 
        asset_ppa=asset_planif_eol[asset_planif_eol['projet'].isin(ppa) == True]

        #subseting: select only asset_id, projet_id, cod, date merchant, date dementelement 
        asset_planif_sol=asset_planif_sol.iloc[:,np.r_[1, 2, 3, 5, 10]]
        asset_planif_eol=asset_planif_eol.iloc[:,np.r_[1, 2, 3, 5, 10]]

        '''
        projet avec des prix qui commencent en cours de 
        l'année:Bois des Fontaines, Repowering Bougainville, Repowering Evit Et Josaphats, Repowering Reclainville
        les prix avant la cod sont egales à 0 puis l'année suivanate, les prix sont disponibles toutes l'année. Pour corriger nous devons 
        créer un df contenant les prix de 2022 à 2023 puis un second de 2023 à 2028
        '''
        price = template_prices
        price_=template_prices
        #To create a list containig projects that are cod during the years 2022 
        out_projets=['Bois des Fontaines', 'Repowering Bougainville', 'Repowering Evit Et Josaphats', 'Repowering Reclainville']

        #Drop rows that contain any value in the list and reset index
        price__ = price_[price_['site'].isin(out_projets) == False]
        price__.reset_index(inplace=True, drop=True)

        #To create a df containg all the prices of all months of the these projects
        data = {'projet_id': ['GO01', 'KEI3', 'GO02', 'GO03'],
                'site':['Bois des Fontaines', 'Repowering Bougainville', 'Repowering Evit Et Josaphats', 'Repowering Reclainville'],
                'jan': ['67.460', '75.120', '73.790', '73.790'],
                'feb': ['67.460', '75.120', '73.790', '73.790'],
                'mar': ['67.460', '75.120', '73.790', '73.790'],
                'apr': ['67.460', '75.120', '73.790', '73.790'],
                'may': ['67.460', '75.120', '73.790', '73.790'],
                'june': ['67.460', '75.120', '73.790', '73.790'],
                'july': ['67.460', '75.120', '73.790', '73.790'],
                'aug': ['67.460', '75.120', '73.790', '73.790'],
                'sep': ['67.460', '75.120', '73.790', '73.790'],
                'oct': ['67.460', '75.120', '73.790', '73.790'],
                'nov': ['67.460', '75.120', '73.790', '73.790'],
                'dec': ['67.460', '75.120', '73.790', '73.790']
        }
        price___=pd.DataFrame(data=data)

        #To merge df  
        frames=[price__, price___]
        price_23_28_id=pd.concat(frames, axis=0)
        #projet_23_28_id.sort_values(by=['projet'], inplace=True, ignore_index=True)
        price_23_28_id.reset_index(inplace=True, drop=True)

        #To join hedge, asset and contract price data frames
        #data 2022
        df_temp=pd.merge(hedge, asset, how='inner', on='projet_id')
        df=pd.merge(df_temp, price, how='inner', on='projet_id')

        #post 2022 (2023 to 2028)
        df_temp2=pd.merge(hedge, asset, how='inner', on='projet_id')
        df2=pd.merge(df_temp2, price_23_28_id, how='inner', on='projet_id')

        #To print columns + indexes
        #list(enumerate(df.columns))
        #Select by range and position   
        df_ = df.iloc[:,np.r_[1, 2, 3, 4, 5, 6, 7, 8, 15, 16, 17, 18, 19, 20:31]]
        #df subseting
        price_oa_cr_temp = df_.loc[df_['type_hedge'] != 'PPA']
        price_oa_cr_temp.reset_index(inplace=True, drop=True)#only 2022 prices data 
        price_ppa = df_.loc[df_['type_hedge'] == 'PPA']
        price_ppa = price_ppa.iloc[:, 0:11]

        #df2= data from 2023 to 2028
        df2_ = df2.iloc[:,np.r_[1, 2, 3, 4, 5, 6, 7, 8, 15, 16, 17, 18, 19, 20:31]]
        #df subseting
        price_oa_cr_temp2 = df2_.loc[df2_['type_hedge'] != 'PPA']
        price_oa_cr_temp2.reset_index(inplace=True, drop=True)

        #OA CR
        '''
        To divide the data frame in 2 distinct df
        price_oa_cr_: contain attributes 
        price_oa_cr: contain prices date  
        '''
        price_oa_cr_temp_ = price_oa_cr_temp.iloc[:, 0:12]
        price_oa_cr_temp2_ = price_oa_cr_temp2.iloc[:, 0:12]

        #price from Jan to dec 2022
        #price jan
        price_oa_cr_1 = pd.concat([price_oa_cr_temp_, price_oa_cr_temp.iloc[:, 12]], axis=1)
        price_oa_cr_1.rename(columns = {'jan':'price'}, inplace = True)
        #price fev
        price_oa_cr_2 = pd.concat([price_oa_cr_temp_, price_oa_cr_temp.iloc[:, 13]], axis=1)
        price_oa_cr_2.rename(columns = {'feb':'price'}, inplace = True)
        #price mar
        price_oa_cr_3 = pd.concat([price_oa_cr_temp_, price_oa_cr_temp.iloc[:, 14]], axis=1)
        price_oa_cr_3.rename(columns = {'mar':'price'}, inplace = True)
        #price avr
        price_oa_cr_4 = pd.concat([price_oa_cr_temp_, price_oa_cr_temp.iloc[:, 15]], axis=1)
        price_oa_cr_4.rename(columns = {'apr':'price'}, inplace = True)
        #price may
        price_oa_cr_5 = pd.concat([price_oa_cr_temp_, price_oa_cr_temp.iloc[:, 16]], axis=1)
        price_oa_cr_5.rename(columns = {'may':'price'}, inplace = True)
        #price jun
        price_oa_cr_6 = pd.concat([price_oa_cr_temp_, price_oa_cr_temp.iloc[:, 17]], axis=1)
        price_oa_cr_6.rename(columns = {'june':'price'}, inplace = True)
        #price jul
        price_oa_cr_7 = pd.concat([price_oa_cr_temp_, price_oa_cr_temp.iloc[:, 18]], axis=1)
        price_oa_cr_7.rename(columns = {'july':'price'}, inplace = True)
        #price aug
        price_oa_cr_8 = pd.concat([price_oa_cr_temp_, price_oa_cr_temp.iloc[:, 19]], axis=1)
        price_oa_cr_8.rename(columns = {'aug':'price'}, inplace = True)
        #price sep
        price_oa_cr_9 = pd.concat([price_oa_cr_temp_, price_oa_cr_temp.iloc[:, 20]], axis=1)
        price_oa_cr_9.rename(columns = {'sep':'price'}, inplace = True)
        #price oct
        price_oa_cr_10 = pd.concat([price_oa_cr_temp_, price_oa_cr_temp.iloc[:, 21]], axis=1)
        price_oa_cr_10.rename(columns = {'oct':'price'}, inplace = True)
        #price nov
        price_oa_cr_11 = pd.concat([price_oa_cr_temp_, price_oa_cr_temp.iloc[:, 22]], axis=1)
        price_oa_cr_11.rename(columns = {'nov':'price'}, inplace = True)
        #price dec
        price_oa_cr_12 = pd.concat([price_oa_cr_temp_, price_oa_cr_temp.iloc[:, 23]], axis=1)
        price_oa_cr_12.rename(columns = {'dec':'price'}, inplace = True)


        #price from Jan 2023 to 2028
        #price jan
        price_oa_cr_1_ = pd.concat([price_oa_cr_temp2_, price_oa_cr_temp2.iloc[:, 12]], axis=1)
        price_oa_cr_1_.rename(columns = {'jan':'price'}, inplace = True)
        #price fev
        price_oa_cr_2_ = pd.concat([price_oa_cr_temp2_, price_oa_cr_temp2.iloc[:, 13]], axis=1)
        price_oa_cr_2_.rename(columns = {'feb':'price'}, inplace = True)
        #price mar
        price_oa_cr_3_ = pd.concat([price_oa_cr_temp2_, price_oa_cr_temp2.iloc[:, 14]], axis=1)
        price_oa_cr_3_.rename(columns = {'mar':'price'}, inplace = True)
        #price avr
        price_oa_cr_4_ = pd.concat([price_oa_cr_temp2_, price_oa_cr_temp2.iloc[:, 15]], axis=1)
        price_oa_cr_4_.rename(columns = {'apr':'price'}, inplace = True)
        #price may
        price_oa_cr_5_ = pd.concat([price_oa_cr_temp2_, price_oa_cr_temp2.iloc[:, 16]], axis=1)
        price_oa_cr_5_.rename(columns = {'may':'price'}, inplace = True)
        #price jun
        price_oa_cr_6_ = pd.concat([price_oa_cr_temp2_, price_oa_cr_temp2.iloc[:, 17]], axis=1)
        price_oa_cr_6_.rename(columns = {'june':'price'}, inplace = True)
        #price jul
        price_oa_cr_7_ = pd.concat([price_oa_cr_temp2_, price_oa_cr_temp2.iloc[:, 18]], axis=1)
        price_oa_cr_7_.rename(columns = {'july':'price'}, inplace = True)
        #price aug
        price_oa_cr_8_ = pd.concat([price_oa_cr_temp2_, price_oa_cr_temp2.iloc[:, 19]], axis=1)
        price_oa_cr_8_.rename(columns = {'aug':'price'}, inplace = True)
        #price sep
        price_oa_cr_9_ = pd.concat([price_oa_cr_temp2_, price_oa_cr_temp2.iloc[:, 20]], axis=1)
        price_oa_cr_9_.rename(columns = {'sep':'price'}, inplace = True)
        #price oct
        price_oa_cr_10_ = pd.concat([price_oa_cr_temp2_, price_oa_cr_temp2.iloc[:, 21]], axis=1)
        price_oa_cr_10_.rename(columns = {'oct':'price'}, inplace = True)
        #price nov
        price_oa_cr_11_ = pd.concat([price_oa_cr_temp2_, price_oa_cr_temp2.iloc[:, 22]], axis=1)
        price_oa_cr_11_.rename(columns = {'nov':'price'}, inplace = True)
        #price dec
        price_oa_cr_12_ = pd.concat([price_oa_cr_temp2_, price_oa_cr_temp2.iloc[:, 23]], axis=1)
        price_oa_cr_12_.rename(columns = {'dec':'price'}, inplace = True)

        #Only price_oa_cr__1 for 2022 
        frames=[price_oa_cr_1, price_oa_cr_2, price_oa_cr_3, price_oa_cr_4, price_oa_cr_5, price_oa_cr_6, price_oa_cr_7, 
               price_oa_cr_8, price_oa_cr_9, price_oa_cr_10, price_oa_cr_11, price_oa_cr_12]

        price_oa_cr__22 = pd.concat(frames, axis=0, ignore_index=False)
        price_oa_cr__22.reset_index(inplace=True, drop=True)
        n_oa_cr =len(price_oa_cr_1)
        #oa cr prices only 
        #time_horizon = time_horizon
        #df1=pd.DataFrame(index=np.arange(89), columns=['date'])#To create an empty df of shape 89 that will contain date column
        #n_oa_cr =price_oa_cr_1.shape[0] 
        #start_date = pd.to_datetime(kwargs['date'] * nbr)
        #d1 = pd.DataFrame()
        #for i in range(0, time_horizon):
            #df_buffer=df1 
            #df_buffer["date"] = start_date
            #d1 = pd.concat([d1, df_buffer], axis=0)
            #start_date= start_date + pd.DateOffset(months=1)
        #reset index    
        #d1.reset_index(drop=True, inplace=True)
        #To concat dates df with oa cr price of only 2022
        #price_oa_cr__22_=pd.concat([price_oa_cr__22, d1], axis=1, ignore_index=False)
        
        price_oa_cr__22_ = create_mini_data_frame(price_oa_cr__22, '01-01-2022', n=n_oa_cr, a=0, b=12*1, date='date')
        price_oa_cr__22_.reset_index(drop=True, inplace=True)

        #To cretae year, trimestre, mois columns
        price_oa_cr__22_['date'] = price_oa_cr__22_['date'].apply(pd.to_datetime)
        price_oa_cr__22_['année'] = price_oa_cr__22_['date'].dt.year
        price_oa_cr__22_['trimestre'] = price_oa_cr__22_['date'].dt.quarter
        price_oa_cr__22_['mois'] = price_oa_cr__22_['date'].dt.month

        #Only from price_oa_cr 2023 to 2028
        frames=[price_oa_cr_1_, price_oa_cr_2_, price_oa_cr_3_, price_oa_cr_4_, price_oa_cr_5_, price_oa_cr_6_, price_oa_cr_7_, 
               price_oa_cr_8_, price_oa_cr_9_, price_oa_cr_10_, price_oa_cr_11_, price_oa_cr_12_]
        
        price_oa_cr_23_28 = pd.concat(frames, axis=0, ignore_index=False)
        price_oa_cr_23_28.reset_index(inplace=True, drop=True)
        #To multiply prices df by 6  
        price_oa_cr_23_28_=pd.concat([price_oa_cr_23_28]*6, ignore_index=True)

        #oa cr prices only 
        #time_horizon = 12*6
        #nbr=price_oa_cr_1_.shape[0]
        #df1=pd.DataFrame(index=np.arange(nbr), columns=['Date'])#To create an empty df of shape 89 that will contain date    
        #start_date = pd.to_datetime(["2023-01-01"] * nbr)#start from 2023 to 2028
        #d1 = pd.DataFrame()
        #for i in range(0, time_horizon):
            #df_buffer=df1 
            #df_buffer["Date"] = start_date
            #d1 = pd.concat([d1, df_buffer], axis=0)
            #start_date= start_date + pd.DateOffset(months=1)
        #Reset index    
        #d1.reset_index(drop=True, inplace=True)
        #To concat dates df with oa cr price of only 2022
        #price_oa_cr_23_28_=pd.concat([price_oa_cr_23_28_, d1], axis=1, ignore_index=False)

        price_oa_cr_23_28_ = create_mini_data_frame(price_oa_cr_23_28_, '01-01-2023', n=n_oa_cr, a=0, b=12*6, date='date')
        price_oa_cr_23_28_.reset_index(inplace=True, drop=True)
        
        #To cretae year, trimestre, mois columns
        price_oa_cr_23_28_['Date'] = price_oa_cr_23_28_['Date'].apply(pd.to_datetime)
        price_oa_cr_23_28_['année'] = price_oa_cr_23_28_['Date'].dt.year
        price_oa_cr_23_28_['trimestre'] = price_oa_cr_23_28_['Date'].dt.quarter
        price_oa_cr_23_28_['mois'] = price_oa_cr_23_28_['Date'].dt.month

        #MERGING VMR OA & CR 2022 AND 2023-2028
        frame=[price_oa_cr__22_, price_oa_cr_23_28_]
        d=pd.concat(frame, axis=0, ignore_index=True)
        d.reset_index(inplace=True, drop=True)
        #To remove price based on date_debut
        #Condition:date column is less (in total seconds) than first date for each projet_id's first date_cod value
        #cond=((d['date'] - d.groupby(['hedge_id', 'projet_id'])['date_debut'].transform('first')).dt.total_seconds())<0
        #d['price'] = np.where(cond,'', d['price'])
        #To remove price based on date_fin
        #cond_2=((d['date'] - d.groupby(['hedge_id', 'projet_id'])['date_fin'].transform('first')).dt.total_seconds())>0
        #d['price'] = np.where(cond_2, '', d['price'])
        #To remove price based on date_dementelement
        #cond_3=((d['date'] - d.groupby(['hedge_id', 'projet_id'])['date_dementelement'].transform('first')).dt.total_seconds())>0
        #d['price'] = np.where(cond_3, '', d['price'])
        
        prices_oa_cr=remove_contract_prices(data=d, sd='date_debut', ed='date_fin', price='price', 
                                            th='type_hedge', date_dementelement='date_dementelement', date='date', projetid='projet_id', 
                                            hedgeid='hedge_id')

        prices_oa_cr=select_columns(prices_oa_cr, 'hedge_id', 'projet_id', 'projet', 'type_hedge', 'date_debut', 
                                    'date_fin', 'date', 'année', 'trimestre', 'mois', 'price') 

        return prices_oa_cr
    except Exception as e:
        print("Contract prices data transformation asset in prod error!")
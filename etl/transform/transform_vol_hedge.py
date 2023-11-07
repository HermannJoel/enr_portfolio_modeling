import pandas as pd
import numpy as np
from datetime import datetime
from datetime import datetime as dt
import sys
import configparser
import os
pd.options.mode.chained_assignment=None
sys.path.append('/mnt/d/local-repo-github/enr_portfolio_modeling')
os.chdir('/mnt/d/local-repo-github/enr_portfolio_modeling/')
from src.utils.functions import*

def transform_hedge_type(hedge):
    try:
        hedge_vmr=hedge.loc[hedge["en_planif"]==False]
        df_oa=hedge_vmr[["hedge_id", "projet_id", "type_hedge", "date_debut", 
                        "date_fin", "date_dementelement", "pct_couverture"]]
        df_oa=df_oa.loc[df_oa["type_hedge"] == "OA"]
            
        df_cr=hedge_vmr[["hedge_id", "projet_id", "type_hedge", "date_debut", 
                        "date_fin", "date_dementelement", "pct_couverture"]]
        
        df_cr=df_cr.loc[(df_cr["type_hedge"] != "OA") & (df_cr["type_hedge"]!= "PPA")]
                
        df_ppa=hedge_vmr[["hedge_id", "projet_id", "type_hedge", "date_debut", 
                        "date_fin", "date_dementelement", "pct_couverture"]]
        df_ppa=df_ppa.loc[df_ppa["type_hedge"] == "PPA"]
        return df_oa, df_cr, df_ppa
    except Exception as e:
        print("Hedge type transformation error!: "+str(e))


def transform_vol_hedge(data_prod, hedge, prod_pct, mean_pct, **kwargs):
    """
    Function to compute P50 & p90 hedge vmr     
    parameters
    ==========
        data_prod (DataFrame) : 
        *args: non-keyworded arguments
            sd (str) : 
                Takes the value of the start of the horizon  dd-mm-yyyy  '01-01-2022'
        **kwargs : keyworded arguments
            oa (DataFrame) : 
            cr (DataFrame) : 
            ppa (DataFrame) : 
            profile (DataFrame) : 
            a (int) : 
                Takes the value 0
            b (int) : 
                Takes the value of the length of our horizon (12*7)
            profile (dictionaries) : 
                The arg takes the value of the production profile
            n (int) : 
                The arg takes the value length of data 
            date (str) : 
                The arg takes the value of date colum label 'date'
    Returns
    =======
    """
    try:
        print('\n')
        print('compute Hedge starts!:\n')
        print('here we go:\n')       
        #To merge cod data and prod data
        df_oa=kwargs['oa'].merge(data_prod, on='projet_id')
        #To merge cod data and prod data
        df_cr=kwargs['cr'].merge(data_prod, on='projet_id')
        #To merge cod data and prod data
        df_ppa=kwargs['ppa'].merge(data_prod, on='projet_id')
        #To determine the number of asset under OA
        n_oa=len(df_oa)
        #To determine the number of asset under CR
        n_cr= len(df_cr)
        #To determine the number of asset under OA
        n_ppa= len(df_ppa)
        #rename prod label with projet_id
        prod_profile=kwargs['profile'].rename(columns=data_prod.set_index('projet')['projet_id'])
        #To define a dict containing prod percentage  
        prod_profile_dict=prod_profile.to_dict()
        #----------  To create OA, CR, PPA hedge dfs ----------#
        print('oa df creation starts:\n')
        d = create_data_frame(df_oa, '01-01-2022', a=0, b=12*7, n=n_oa, date='date', 
                          profile=prod_profile_dict)
        d.reset_index(inplace=True, drop=True)
        print('oa df creation ends!:\n')
        print('cr df creation starts:\n')
        d2 = create_data_frame(df_cr, '01-01-2022', a=0, b=12*7, n=n_cr, date='date', 
                           profile=prod_profile_dict)
        d2.reset_index(inplace=True, drop=True)
        print('cr df creation ends!:\n')
        print('ppa df creation starts:\n')
        d3 = create_data_frame(df_ppa, '01-01-2022', a=0, b=12*7, n=n_ppa, date='date', 
                            profile=prod_profile_dict)
        d3.reset_index(inplace=True, drop=True)
        print('ppa df creation ends!:\n')    
        #---------- OA  ----------#
        #Multiply P50 with pct_couverture to obtain adjusted values by hedge percentage
        d["p50_adj"] = adjusted_by_pct(d, col1='p50_adj', col2='pct_couverture')
        d["p90_adj"] = adjusted_by_pct(d, col1='p90_adj', col2='pct_couverture')     
        #To convert date
        d["date_debut"] = pd.to_datetime(d["date_debut"])
        d["date"] = pd.to_datetime(d["date"])
        d["date_fin"] = pd.to_datetime(d["date_fin"])
        d["date_dementelement"] = pd.to_datetime(d["date_dementelement"])
        #Extract year/month/day
        d['année'] = d['date'].dt.year
        d['trim'] = d['date'].dt.to_period('Q').dt.strftime("Q%q-%y")
        d['mois'] = d['date'].dt.month
            
        #---------- CR  ----------#
        #Multiply P50 with pct_couverture to obtain adjusted values by hedge percentage
        d2["p50_adj"] = adjusted_by_pct(d2, col1='p50_adj', col2='pct_couverture')
        d2["p90_adj"] = adjusted_by_pct(d2, col1='p90_adj', col2='pct_couverture')    
        #To create new columns
        d2["date_debut"] = pd.to_datetime(d2["date_debut"])
        d2["date"] = pd.to_datetime(d2["date"])
        d2["date_fin"] = pd.to_datetime(d2["date_fin"])
        d2["date_dementelement"] = pd.to_datetime(d2["date_dementelement"])
        #Extract year/month/day values from date
        d2['année'] = d2['date'].dt.year
        d2['trim'] = d2['date'].dt.to_period('Q').dt.strftime("Q%q-%y")
        d2['mois'] = d2['date'].dt.month
            
        #---------- PPA  ----------#
        #Multiply P50 with pct_couverture to obtain adjusted values by hedge percentage
        d3["p50_adj"] = adjusted_by_pct(d3, col1='p50_adj', col2='pct_couverture')
        d3["p90_adj"] = adjusted_by_pct(d3, col1='p90_adj', col2='pct_couverture') 
        #To convert columns to datetime
        d3["date_debut"] = pd.to_datetime(d3["date_debut"])
        d3["date"] = pd.to_datetime(d3["date"])
        d3["date_fin"] = pd.to_datetime(d3["date_fin"])
        d3["date_dementelement"] = pd.to_datetime(d3["date_dementelement"])
        #Extract year/month/day from date
        d3['année'] = d3['date'].dt.year
        d3['trim'] = d3['date'].dt.to_period('Q').dt.strftime("Q%q-%y")
        d3['mois'] = d3['date'].dt.month
            
        #To remove p50, p90, type_hedge based on start_date and end_date
        #---------- OA  ----------#
        res=RemoveP50P90TypeHedge(d, sd='date_debut',ed='date_fin', p50='p50_adj', 
                                p90='p90_adj', th='type_hedge', date='date', 
                                projetid='projet_id', hedgeid='hedge_id')
        res=SelectColumns(res, 'hedge_id', 'projet_id', 'projet', 'type_hedge', 'date', 
                        'année', 'trim', 'mois', 'p50_adj', 'p90_adj')
        #To export results as a data frame
        #res.to_csv(path_dir_temp + 'hedge_oa.txt', index=False, sep=';') 
        #---------- CR  ----------#
        res2=RemoveP50P90TypeHedge(d2, sd='date_debut', ed='date_fin', p50='p50_adj', 
                                p90='p90_adj', th='type_hedge', date='date', 
                                projetid='projet_id', hedgeid='hedge_id')

        res2=SelectColumns(res2, 'hedge_id', 'projet_id', 'projet', 'type_hedge', 'date', 
                            'année', 'trim', 'mois', 'p50_adj', 'p90_adj')
        #To export results as a data frame
        #res2.to_csv(path_dir_temp + 'hedge_cr.txt', index=False, sep=';')
        #---------- PPA  ----------#
        res3=remove_p50_p90_type_hedge(d3, sd='date_debut', ed='date_fin', p50='p50_adj', 
                                    p90='p90_adj', th='type_hedge', date='date', 
                                    projetid='projet_id', hedgeid='hedge_id')

        res3=select_columns(res3, 'hedge_id', 'projet_id', 'projet', 'type_hedge', 'date', 
                        'année', 'trim', 'mois', 'p50_adj', 'p90_adj')
        #To export results as a data frame
        #res3.to_csv(path_dir_temp + 'hedge_ppa.txt', index=False, sep=';')
        
        #To merge hedge OA, CR, and PPA dfs
        hedge_vmr=merge_data_frame(res, res2, res3)
        #Export p50_p90_hedge_vmr as p50_p90_hedge_vmr.xlsx
        #hedge_vmr=hedge_vmr.assign(id=[1 + i for i in xrange(len(hedge_vmr))])[['id'] + hedge_vmr.columns.tolist()]
        hedge_vmr=hedge_vmr[['hedge_id', 'projet_id', 'projet', 'type_hedge', 
                            'date', 'année', 'trim', 'mois', 'p50_adj', 'p90_adj']]
        
        print('compute hedge assets in planif starts!:\n')  
        hedge_planif=hedge.loc[hedge["en_planif"]==True]
        #8760=24*365(operating hours).To calculate p50/p90 in mw/h of assets in planification.mw*8760*charging factor 
        #wind power
        hedge_planif.loc[(hedge_planif['technologie']=='éolien') & (hedge_planif['en_planif']==True), 'p50']=hedge_planif["puissance_installée"]*8760*0.25
        hedge_planif.loc[(hedge_planif['technologie']=='éolien') & (hedge_planif['en_planif']==True), 'p90']=hedge_planif["puissance_installée"]*8760*0.20
        #solar
        hedge_planif.loc[(hedge_planif['technologie']=='solaire') & (hedge_planif['en_planif']==True), 'p50']=hedge_planif["puissance_installée"]*8760*0.15
        hedge_planif.loc[(hedge_planif['technologie']=='solaire') & (hedge_planif['en_planif']==True), 'p90']=hedge_planif["puissance_installée"]*8760*0.13

        #To calculate p50 p90 adjusted by the pct_couverture
        hedge_planif["p50"]=hedge_planif["p50"]*hedge_planif["pct_couverture"]
        hedge_planif["p90"]=hedge_planif["p90"]*hedge_planif["pct_couverture"]

        prod_planif_solar=hedge_planif.loc[(hedge_planif['technologie'] == "solaire") & (hedge_planif['en_planif'] == True)]
        prod_planif_solar.reset_index(drop = True, inplace=True)
        prod_planif_wp=hedge_planif.loc[(hedge_planif['technologie'] == "éolien") & (hedge_planif['en_planif'] == True)]
        prod_planif_wp.reset_index(drop = True, inplace=True)

        #To determine the number of solar and eolien
        n_sol=len(prod_planif_solar)
        n_wp=len(prod_planif_wp)
        mean_pct_sol=mean_pct.iloc[:,[0, 1]]
        mean_pct_wp=mean_pct.iloc[:,[0,-1]]
            
        print('create solar & wind power dfs:\n')
        #create a df solar
        d1_ = create_mini_data_frame(prod_planif_solar, '01-01-2022', n=n_sol, a=0, b=12*7, date='date')   
        d1_.reset_index(drop=True, inplace=True)
        #create a df wind power
        d2_ = create_mini_data_frame(prod_planif_wp, '01-01-2022', n=n_wp, a=0, b=12*7, date='date')   
        d2_.reset_index(drop=True, inplace=True)
        #Solar
        mean_pct_sol=mean_pct_sol.assign(mth=[1 + i for i in xrange(len(mean_pct_sol))])[['mth'] + mean_pct_sol.columns.tolist()]
        #To calculate adjusted p50 and p90 solar adusted by the mean profile 
        s=mean_pct_sol.set_index('mth')['m_pct_solaire']
        pct = pd.to_datetime(d1_['date']).dt.month.map(s)
        d1_['p50_adj'] = -d1_['p50'] * pct
        d1_['p90_adj'] = -d1_['p90'] * pct
        #To create new columns année et mois
        d1_["date_debut"] = pd.to_datetime(d1_["date_debut"])
        d1_["date_fin"] = pd.to_datetime(d1_["date_fin"])
        d1_["date_dementelement"] = pd.to_datetime(d1_["date_dementelement"])
        d1_['année'] = d1_['date'].dt.year
        d1_['trim'] = d1_['date'].dt.to_period('Q').dt.strftime("Q%q-%y")
        d1_['mois'] = d1_['date'].dt.month
        d1_ = d1_[['hedge_id','projet_id', 'projet', 'type_hedge', 'date_debut', "date_fin", 
                'date_dementelement', 'date', 'année', 'trim', 'mois', 'p50_adj', 'p90_adj']]    
        #wind power
        #create a mth column containing number of month
        mean_pct_wp = mean_pct_wp.assign(mth=[1 + i for i in xrange(len(mean_pct_wp))])[['mth'] + mean_pct_wp.columns.tolist()]
        #To calculate adjusted p50 and p90 wp (adjusted with p50)
        s2=mean_pct_wp.set_index('mth')['m_pct_eolien']
        pct=pd.to_datetime(d2_['date']).dt.month.map(s2)
        d2_['p50_adj'] = -d2_['p50'] * pct
        d2_['p90_adj'] = -d2_['p90'] * pct
        #To create new columns
        d2_["date_debut"] = pd.to_datetime(d2_["date_debut"])
        d2_["date_fin"] = pd.to_datetime(d2_["date_fin"])
        d2_['année'] = d2_['date'].dt.year
        d2_['trim'] = d2_['date'].dt.to_period('Q').dt.strftime("Q%q-%y")
        d2_['mois'] = d2_['date'].dt.month
        d2_= d2_[['hedge_id','projet_id', 'projet', 'type_hedge', 'date_debut', 'date_fin', 
                'date_dementelement', 'date', 'année', 'trim', 'mois', 'p50_adj', 'p90_adj']]

        #To remove p50 p90 based on date_debut
        res= remove_p50_p90_type_hedge(data=d1_, sd='date_debut', ed='date_fin', p50='p50_adj', 
                                    p90='p90_adj', th='type_hedge', date='date', 
                                    projetid='projet_id', hedgeid='hedge_id')
        hedge_solar=select_columns(res, 'hedge_id', 'projet_id', 'projet', 'type_hedge', 'date', 
                                    'année', 'trim', 'mois', 'p50_adj', 'p90_adj')
        #To remove p50 p90 based on date_debut
        res2=remove_p50_p90_type_hedge(data=d2_, sd='date_debut', ed='date_fin', p50='p50_adj', 
                                    p90='p90_adj', th='type_hedge', date='date', 
                                    projetid='projet_id', hedgeid='hedge_id')
        hedge_wp=select_columns(res2, 'hedge_id', 'projet_id', 'projet', 'type_hedge', 'date', 
                                'année', 'trim', 'mois', 'p50_adj', 'p90_adj')
            
        #To merge hedge_vmr and hedge_planif
        hedge_vmr_planif = merge_data_frame(hedge_vmr, hedge_solar, hedge_wp)
        hedge_vmr_planif=hedge_vmr_planif.assign(id=[1 + i for i in xrange(len(hedge_vmr_planif))])[['id'] + hedge_vmr_planif.columns.tolist()]
        print('compute hedge ends!:\n') 
        
        return hedge_vmr_planif
    except Exception as e:
        print("vol hedge transformation error!: "+str(e))
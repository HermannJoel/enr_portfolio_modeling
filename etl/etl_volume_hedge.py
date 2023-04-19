import pandas as pd
import numpy as np
xrange = range
from datetime import datetime
import sys
import os
import configparser
pd.options.mode.chained_assignment = None
# adding etls/functions to the system path
sys.path.insert(0, 'D:/git-local-cwd/Data-Engineering-Projects/blx_mdp_data-eng/etls/functions')
from etl_functions import (RemoveP50P90TypeHedge, CreateDataFrame, 
                           MergeDataFrame, AdjustedByPct, ChooseCwd, 
                           ReadExcelFile, SelectColumns,CreateMiniDataFrame
                          )

#Load Config
config_file=os.path.join(os.path.dirname("__file__"), 'Config/config.ini')
config=configparser.ConfigParser(allow_no_value=True)
config.read(config_file)

dest_dir=os.path.join(os.path.dirname("__file__"),config['develop']['dest_dir'])
src_dir=os.path.join(os.path.dirname("__file__"),config['develop']['src_dir'])

          
def Extract(prod_path, prod_pct_path, mean_pct_path, asset_path, hedge_path):
    ''' Function to extract excel files.
    Parameters
    ==========
    prod_path: str
        prod excel file path
    prod_pct_path: str
        prod excel file path
    mean_pct_path: str
        prod excel file path
    asset_path: str
        asset template file path
    hedge_path: str
        hedge template excel file path
        
    Returns
    =======
    df_prod: DataFrame
        prod dataframe
    df_prod_pct: DataFrame
        prod profile dataframe
    df_mean_pct: DataFrame
        mean profile dataframe
    df_asset: DataFrame
        asset dataframe
    df_hedge: DataFrame
        hedge dataframe
    '''
    df_prod=ReadExcelFile(prod_path, sheet_name="productible")
    df_prod_pct=ReadExcelFile(prod_pct_path, sheet_name="profile_id")
    df_mean_pct=ReadExcelFile(mean_pct_path, sheet_name="mean_profile")
    df_asset=ReadExcelFile(asset_path)
    df_hedge=ReadExcelFile(hedge_path)
    
    return df_prod, df_prod_pct, df_mean_pct, df_asset, df_hedge
 
def Transform_(hedge):
    try:
        hedge_vmr=hedge.loc[hedge["en_planif"]=="Non"]
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
        print("Data transformation error!: "+str(e))


def TransformHedge(data_prod, hedge, prod_pct, mean_pct, **kwargs):
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
        d=CreateDataFrame(df_oa, '01-01-2022', a=0, b=12*7, n=n_oa, date='date', 
                          profile=prod_profile_dict)
        d.reset_index(inplace=True, drop=True)
        print('oa df creation ends!:\n')
        print('cr df creation starts:\n')
        d2=CreateDataFrame(df_cr, '01-01-2022', a=0, b=12*7, n=n_cr, date='date', 
                           profile=prod_profile_dict)
        d2.reset_index(inplace=True, drop=True)
        print('cr df creation ends!:\n')
        print('ppa df creation starts:\n')
        d3=CreateDataFrame(df_ppa, '01-01-2022', a=0, b=12*7, n=n_ppa, date='date', 
                            profile=prod_profile_dict)
        d3.reset_index(inplace=True, drop=True)
        print('ppa df creation ends!:\n')    
        #---------- OA  ----------#
        #Multiply P50 with pct_couverture to obtain adjusted values by hedge percentage
        d["p50_adj"]=AdjustedByPct(d, col1='p50_adj', col2='pct_couverture')
        d["p90_adj"]=AdjustedByPct(d, col1='p90_adj', col2='pct_couverture')     
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
        d2["p50_adj"]=AdjustedByPct(d2, col1='p50_adj', col2='pct_couverture')
        d2["p90_adj"]=AdjustedByPct(d2, col1='p90_adj', col2='pct_couverture')    
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
        d3["p50_adj"]=AdjustedByPct(d3, col1='p50_adj', col2='pct_couverture')
        d3["p90_adj"]=AdjustedByPct(d3, col1='p90_adj', col2='pct_couverture') 
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
        res3=RemoveP50P90TypeHedge(d3, sd='date_debut', ed='date_fin', p50='p50_adj', 
                                    p90='p90_adj', th='type_hedge', date='date', 
                                    projetid='projet_id', hedgeid='hedge_id')

        res3=SelectColumns(res3, 'hedge_id', 'projet_id', 'projet', 'type_hedge', 'date', 
                        'année', 'trim', 'mois', 'p50_adj', 'p90_adj')
        #To export results as a data frame
        #res3.to_csv(path_dir_temp + 'hedge_ppa.txt', index=False, sep=';')
        
        #To merge hedge OA, CR, and PPA dfs
        hedge_vmr=MergeDataFrame(res, res2, res3)
        #Export p50_p90_hedge_vmr as p50_p90_hedge_vmr.xlsx
        #hedge_vmr=hedge_vmr.assign(id=[1 + i for i in xrange(len(hedge_vmr))])[['id'] + hedge_vmr.columns.tolist()]
        hedge_vmr=hedge_vmr[['hedge_id', 'projet_id', 'projet', 'type_hedge', 
                            'date', 'année', 'trim', 'mois', 'p50_adj', 'p90_adj']]
        
        print('compute hedge assets in planif starts!:\n')  
        hedge_planif=hedge.loc[hedge["en_planif"]=="Oui"]
        #8760=24*365(operating hours).To calculate p50/p90 in mw/h of assets in planification.mw*8760*charging factor 
        #wind power
        hedge_planif.loc[(hedge_planif['technologie']=='éolien') & (hedge_planif['en_planif']=='Oui'), 'p50']=hedge_planif["puissance_installée"]*8760*0.25
        hedge_planif.loc[(hedge_planif['technologie']=='éolien') & (hedge_planif['en_planif']=='Oui'), 'p90']=hedge_planif["puissance_installée"]*8760*0.20
        #solar
        hedge_planif.loc[(hedge_planif['technologie']=='solaire') & (hedge_planif['en_planif']=='Oui'), 'p50']=hedge_planif["puissance_installée"]*8760*0.15
        hedge_planif.loc[(hedge_planif['technologie']=='solaire') & (hedge_planif['en_planif']=='Oui'), 'p90']=hedge_planif["puissance_installée"]*8760*0.13

        #To calculate p50 p90 adjusted by the pct_couverture
        hedge_planif["p50"]=hedge_planif["p50"]*hedge_planif["pct_couverture"]
        hedge_planif["p90"]=hedge_planif["p90"]*hedge_planif["pct_couverture"]

        prod_planif_solar=hedge_planif.loc[(hedge_planif['technologie'] == "solaire") & (hedge_planif['en_planif'] == 'Oui')]
        prod_planif_solar.reset_index(drop = True, inplace=True)
        prod_planif_wp=hedge_planif.loc[(hedge_planif['technologie'] == "éolien") & (hedge_planif['en_planif'] == 'Oui')]
        prod_planif_wp.reset_index(drop = True, inplace=True)

        #To determine the number of solar and eolien
        n_sol=len(prod_planif_solar)
        n_wp=len(prod_planif_wp)
        mean_pct_sol=mean_pct.iloc[:,[0, 1]]
        mean_pct_wp=mean_pct.iloc[:,[0,-1]]
            
        print('create solar & wind power dfs:\n')
        #create a df solar
        d1_=CreateMiniDataFrame(prod_planif_solar, '01-01-2022', n=n_sol, a=0, b=12*7, date='date')   
        d1_.reset_index(drop=True, inplace=True)
        #create a df wind power
        d2_=CreateMiniDataFrame(prod_planif_wp, '01-01-2022', n=n_wp, a=0, b=12*7, date='date')   
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
        res=RemoveP50P90TypeHedge(data=d1_, sd='date_debut', ed='date_fin', p50='p50_adj', 
                                    p90='p90_adj', th='type_hedge', date='date', 
                                    projetid='projet_id', hedgeid='hedge_id')
        hedge_solar=SelectColumns(res, 'hedge_id', 'projet_id', 'projet', 'type_hedge', 'date', 
                                    'année', 'trim', 'mois', 'p50_adj', 'p90_adj')
        #To remove p50 p90 based on date_debut
        res2=RemoveP50P90TypeHedge(data=d2_, sd='date_debut', ed='date_fin', p50='p50_adj', 
                                    p90='p90_adj', th='type_hedge', date='date', 
                                    projetid='projet_id', hedgeid='hedge_id')
        hedge_wp=SelectColumns(res2, 'hedge_id', 'projet_id', 'projet', 'type_hedge', 'date', 
                                'année', 'trim', 'mois', 'p50_adj', 'p90_adj')
            
        #To merge hedge_vmr and hedge_planif
        hedge_vmr_planif=MergeDataFrame(hedge_vmr, hedge_solar, hedge_wp)
        hedge_vmr_planif=hedge_vmr_planif.assign(id=[1 + i for i in xrange(len(hedge_vmr_planif))])[['id'] + hedge_vmr_planif.columns.tolist()]
        print('compute hedge ends!:\n') 
        
        return hedge_vmr_planif
    except Exception as e:
        print("Hedge transformation error!: "+str(e))


def Load(dest_dir, src_flow, file_name, file_extension):
    """Function to load data as excle file     
    parameters
    ==========
    dest_dir (str) :
        target folder path
    src_flow (DataFrame) :
        data frame returned by transform function        
    file_name (str) : 
        destination file name
    exemple
    =======
    Load(dest_dir, template_asset_without_prod, 'template_asset', '.csv')
    >>> to load template_asset_without_prod in dest_dir as template_asset.csv 
    """
    try:
        if file_extension in ['.xlsx', '.xls', '.xlsm', '.xlsb', '.odf', '.ods', '.odt']:
            src_flow.to_excel(dest_dir+file_name+file_extension, index=False, float_format="%.4f")
        else: 
            src_flow.to_csv(dest_dir+file_name+file_extension, index=False, float_format="%.4f", encoding='utf-8-sig')
        print("Data loaded succesfully!")
    except Exception as e:
        print("Data load error!: "+str(e))
        
    





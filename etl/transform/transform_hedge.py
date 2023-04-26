import pandas as pd
import numpy as np
from datetime import datetime
import sys
import configparser
import os
pd.options.mode.chained_assignment=None
os.chdir('D:/local-repo-github/enr_portfolio_modeling/')
from src.utils.functions import*


def transform_hedge(hedge_vmr, hedge_planif, **kwargs):
    """
    udf Function to generate template hedge
    Parameters
    ===========
    **kwargs
        hedge_vmr: DataFrame
                
        hedge_planif: DataFrame
    Returns
    =======
    hedge_template: DataFrame
        template asset dataframe
    """
    try:
        #===============     Hedge vmr     =======================
        #To create hedge df with vmr data
        df_hedge_vmr=hedge_vmr
        df_hedge_vmr["profil"]=np.nan
        df_hedge_vmr["pct_couverture"]=np.nan
        df_hedge_vmr["contrepartie"]=np.nan
        df_hedge_vmr["pays_contrepartie"]=np.nan
        df_hedge_vmr.rename(columns={"cod":"date_debut", "date_merchant":"date_fin"}, inplace = True)
        df_hedge_vmr = df_hedge_vmr[["id", "hedge_id", "projet_id", 
                                           "projet", "type_hedge", "date_debut", 
                                           "date_fin", "profil", "pct_couverture", 
                                           "contrepartie", "pays_contrepartie"]]

        ppa_vmr = ["NIBA" , "CHEP", "ALBE", "ALME", "ALMO", "ALVE", "PLOU"]

        df_hedge_vmr["type_hedge"] = df_hedge_vmr["type_hedge"].str.replace("FiT", "OA")
        df_hedge_vmr.loc[df_hedge_vmr.projet_id.isin(ppa_vmr) == True, "type_hedge"] = "PPA" 

        df_hedge_vmr.loc[df_hedge_vmr['type_hedge'] == "OA", "pct_couverture"] = 1
        df_hedge_vmr.loc[df_hedge_vmr['type_hedge'] != "OA", "pct_couverture"] = 1
        df_hedge_vmr.loc[df_hedge_vmr['type_hedge'] == "PPA", "pct_couverture"] = 1

        #===============     Hedge Planif     =======================
        #To import hedge_planif data. this was generated when creating the template_asset.
        df_hedge_planif=hedge_planif
        df_hedge_planif["type_hedge"] = "CR"
        df_hedge_planif["profil"] = np.nan
        df_hedge_planif["pct_couverture"] = np.nan
        df_hedge_planif["contrepartie"] = np.nan
        df_hedge_planif["pays_contrepartie"] = np.nan
        df_hedge_planif.rename(columns={"cod":"date_debut", "date_merchant":"date_fin"}, inplace = True)
        df_hedge_planif = df_hedge_planif[["id", "hedge_id", "projet_id", 
                                           "projet", "type_hedge", "date_debut", 
                                           "date_fin", "profil", "pct_couverture", 
                                           "contrepartie", "pays_contrepartie"]]
        ppa_planif = ["SE19", "SE07"]
        df_hedge_planif.loc[df_hedge_planif.projet_id.isin(ppa_planif) == True, "type_hedge"] = "PPA"
        df_hedge_planif.loc[df_hedge_planif['type_hedge'] == "CR", "pct_couverture"] = 1
        df_hedge_planif.loc[df_hedge_planif['type_hedge'] == "PPA", "pct_couverture"] = 1

        #To merge both data frame
        frames = [df_hedge_vmr, df_hedge_planif]
        hedge_template = pd.concat(frames)
        hedge_template.reset_index(inplace=True, drop=True)
        hedge_template.drop("id", axis=1, inplace=True)
        hedge_template=hedge_template.assign(id=[1 + i for i in xrange(len(hedge_template))])[['id'] + hedge_template.columns.tolist()]
        
        return hedge_template
    
    except Exception as e:
        print("Template hedge transformation error!: "+str(e))
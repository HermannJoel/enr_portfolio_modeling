import pandas as pd
import numpy as np
import datetime as datetime
xrange = range
import configparser
import sys
from pandasql import sqldf
pysqldf=lambda q: sqldf(q, globals())
import os
pd.options.mode.chained_assignment = None

#Load Config
config_file=os.path.join(os.path.dirname("__file__"), 'Config/config.ini') 
config=configparser.ConfigParser(allow_no_value=True)
config.read(config_file)

# adding etls/functions to the system path
sys.path.insert(0, 'D:/git-local-cwd/Data-Engineering-Projects/blx_mdp_data-eng/etls/functions')
from etl_functions import (RemoveP50P90TypeHedge, CreateDataFrame, 
                                     MergeDataFrame, AdjustedByPct, ChooseCwd,
                                     RemoveP50P90, ReadExcelFile, SelectColumns,CreateMiniDataFrame)

dest_dir=os.path.join(os.path.dirname("__file__"), config['develop']['dest_dir'])
temp_dir=os.path.join(os.path.dirname("__file__"),config['develop']['temp_dir'])

def Extract(asset_vmr_path, asset_planif_path):
    ''' Function to extract excel files.
    Parameters
    ==========
    asset_vmr_path: str
        path excel file containing data asset in prod
    asset_planif_path: str
        path excel file containing data asset in planif    
    Returns
    =======
    df_asset_vmr: DataFrame
        asset vmr dataframe
    df_planif: DataFrame
        asset planif dataframe
    '''
    try:
        df_asset_vmr=ReadExcelFile(asset_vmr_path, sheet_name="vmr", header=0)
        df_asset_planif=ReadExcelFile(asset_planif_path, sheet_name="Planification", header=20, 
                                      usecols=['#', 'Nom', 'Technologie', 'Puissance totale (pour les  repowering)', 
                                               'date MSI depl', "date d'entrée dans statut S", 'Taux de réussite'])
        return df_asset_vmr, df_asset_planif 
    except Exception as e:
        print("Data Extraction error!: "+str(e))


def Transform(data_asset_vmr, data_asset_planif, **kwargs):
    """
    udf Function to generate template asset.
    Parameters
    ===========
    **kwargs
        data_asset_vmr: DataFrame
                
        data_asset_planif: DataFrame
    Returns
    =======
    template_asset_w/o_prod: DataFrame
        template asset without productibles dataframe
    
    """
    print('create template asset starts!:\n')
    try:
        #To create a list containing parcs that are out of service
        out_projets = ["Bougainville", "Cham Longe 1", "Evits et Josaphats", "Remise Reclainville", 
                        "Evits et Josaphats", "Remise Reclainville", "Maurienne / Gourgançon", "La Bouleste", 
                        "Cham Longe 1 - off", "Remise Reclainville - off", "Evits et Josaphats - off", "Bougainville - off", 
                        "Maurienne / Gourgançon - off", "Saint-André - off"]
        
        df=data_asset_vmr
        df.rename(columns = {"Alias":"projet", "Technologie":"technologie", 
                             "COD":"cod", "MW 100%":"mw", "Taux succès":"taux_succès", 
                              "MW pondérés":"puissance_installée", "EOH":"eoh", 
                             "Mécanisme":"type_hedge", "Début FiT ajusté":"date_debut", 
                              "Date Merchant":"date_merchant"}, inplace = True)

        #Drop rows that contain any value in the list and reset index
        df = df[df['Parc '].isin(out_projets) == False]
        df.reset_index(inplace=True, drop=True)

        #To select rows where projet name is NaN
        df=df[df['projet'].notna()]
        df.reset_index(inplace=True, drop=True)

        #To correct eolien & solar orthograph
        df["technologie"] = df["technologie"].str.replace("Eolien", "éolien")
        df["technologie"] = df["technologie"].str.replace("PV", "solaire")
        #To set "taux_succès" of all parcs in exploitation equal to 100%
        df["taux_succès"] = 1
        #To compute pondered "puissance_installée"
        df["puissance_installée"] = df["mw"] * df["taux_succès"]
        #To set "date_dementelement" 6 months before "date_msi"
        df["date_dementelement"] = df["date_msi"] - pd.DateOffset(months=6)
        #To create "en_planif" column Bolean: Non=for parc already in exploitation/Oui=projet in planification
        df["en_planif"] = "Non"

        df = df.assign(asset_id=[1 + i for i in xrange(len(df))])[['asset_id'] + df.columns.tolist()]
        df = df.assign(id=[1 + i for i in xrange(len(df))])[['id'] + df.columns.tolist()]

        df_asset = df[["id","asset_id", "projet_id", "projet", "technologie", "cod", "mw", 
                       "taux_succès", "puissance_installée", "type_hedge", "date_debut", 
                       "eoh", "date_merchant", "date_dementelement", "repowering", 
                        "date_msi", "en_planif"]]

        #To create a df containing projects with a cod>= 2023 
        vmr_to_planif = df_asset[df_asset['cod'] > (datetime.datetime.today() + pd.offsets.YearEnd()).strftime('%Y-%m-%d')]
        vmr_to_planif = vmr_to_planif[["id", "asset_id", "projet_id", "projet", "technologie", "cod", "mw", "taux_succès", 
                                        "puissance_installée", "eoh", "date_merchant", "date_dementelement", 
                                        "repowering", "date_msi", "en_planif"]]

        #To create a df containing projets already in exploitation
        df_asset=df_asset[df_asset['cod'] <= (datetime.datetime.today() + pd.offsets.YearEnd()).strftime('%Y-%m-%d')]
        project_names=df_asset[["asset_id", "projet_id", "projet"]]
        project_names.rename(columns={"projet_id":"code", "projet":"projet_name"}, inplace=True)

        #To select specific rows to create a hedge df
        hedge_vmr=df_asset[["id", "projet_id", "projet", "technologie", "type_hedge", "cod", 
                            "date_merchant", "date_dementelement", "puissance_installée", "en_planif"]]
        #To create a column containing hedge_id
        hedge_vmr = hedge_vmr.assign(hedge_id=[1 + i for i in xrange(len(hedge_vmr))])[['hedge_id'] + hedge_vmr.columns.tolist()]
        #To select specific columns 
        hedge_vmr = hedge_vmr[["id", "hedge_id", "projet_id", "projet", "technologie", "type_hedge", "cod", 
                               "date_merchant", "date_dementelement", "puissance_installée", "en_planif"]]
        #Select specific columns to create asset template    
        df_asset_vmr=df_asset[["id", "asset_id", "projet_id", "projet", "technologie", 
                               "cod", "mw", "taux_succès","puissance_installée", "eoh", 
                               "date_merchant", "date_dementelement", "repowering", 
                               "date_msi", "en_planif"]]

        #To make export as excel files
        vmr_to_planif.to_excel(temp_dir+"asset_vmr_to_planif.xlsx", index=False, float_format="%.3f")#This file contains data of assets still in planification but were in assets in planification. 
        project_names.to_excel(temp_dir + "project_names.xlsx", index=False)
        hedge_vmr.to_excel(temp_dir + "hedge_vmr.xlsx", index=False, float_format="%.3f")#This file contains data tha will be used to create hedge template of assets in production.
        #This part of the code is to preprocess data of assets in planification
        #==============================================================================
        #=============== Data preprocessing  of Asset in planification  ===============
        #==============================================================================
        #To import data frame containing projects in planification
        df_=data_asset_planif
        #To drop all projects with "Nom" as optimisation 
        rows_to_drop = sqldf("select * from df_ where Nom like 'optimisation%';", locals())
        rows_to_drop = list(rows_to_drop['Nom'])
        #To drop all projects with "Nom" as Poste
        rows_to_drop2 = sqldf("select * from df_ where Nom like 'Poste%';", locals())
        rows_to_drop2 = list(rows_to_drop2['Nom'])
        #To drop all projects with "Nom" as Stockage 
        rows_to_drop3 = sqldf("select * from df_ where Nom like 'Stockage%';", locals())
        rows_to_drop3 = list(rows_to_drop3['Nom'])
        #To drop all projects with "Nom" as Regul 
        rows_to_drop4 = sqldf("select * from df_ where Nom like 'Régul%';", locals())
        rows_to_drop4 = list(rows_to_drop4['Nom'])

        #To rename columns
        df_.rename(columns = {'#':'projet_id', 'Nom':'projet', 'Technologie':'technologie', 
                              'Puissance totale (pour les  repowering)':'mw','date MSI depl':'date_msi', 
                              'Taux de réussite':'taux_succès'}, inplace=True)

        #drop optimisation
        df_ = df_[df_.projet.isin(rows_to_drop) == False]
        #drop projects poste de...
        df_ = df_[df_.projet.isin(rows_to_drop2) == False]
        #drop projects Stockage de...
        df_ = df_[df_.projet.isin(rows_to_drop3) == False]
        #drop projects Regul de...
        df_ = df_[df_.projet.isin(rows_to_drop4) == False]
        #To select all projets where technologie is not autre 
        df_ = df_.loc[df_['technologie'] != 'autre']


        df_['date_msi']=pd.to_datetime(df_["date_msi"])

        #To fill n/a of date_msi column with with date today + 50 years
        df_["date_msi"].fillna((datetime.datetime.today() + pd.DateOffset(years=50)).strftime('%Y-%m-%d'), inplace=True)

        #To select projects in planif with a cod date less than 2023. These projects should be moved to projects in prod 
        df_to_asset_vmr = df_[df_['date_msi'] < (datetime.datetime.today() + pd.offsets.YearEnd()).strftime('%Y-%m-%d')]


        #To select only data with cod superior to year's end date
        filt = df_['date_msi'] > (datetime.datetime.today() + pd.offsets.YearEnd()).strftime('%Y-%m-%d') 
        df_ = df_.loc[filt]

        #To select rows where Nom is NaN
        df_ = df_[df_['projet'].notna()]
        df_to_asset_vmr.reset_index(inplace=True, drop=True)
        df_.reset_index(inplace=True, drop=True)

        #To set date cod equals to date msi
        df_['cod'] = df_['date_msi']
        df_to_asset_vmr['cod'] = df_to_asset_vmr['date_msi']

        #To fill n/a in taux_succès column with default value
        df_["taux_succès"].fillna(0.599, inplace=True)
        df_to_asset_vmr['taux_succès'].fillna(1, inplace=True)

        #To calculate mw100%
        df_['puissance_installée']=df_['mw'] * df_["taux_succès"]
        df_to_asset_vmr['puissance_installée'] = df_to_asset_vmr["mw"] * df_to_asset_vmr["taux_succès"]
        #To create a column called eoh
        df_['eoh'] = np.nan
        df_to_asset_vmr['eoh'] = np.nan

        #To set a date merchant as date cod + 20 years 
        df_['date_merchant'] = df_["cod"] + pd.DateOffset(years=20) 
        df_to_asset_vmr['date_merchant'] = df_to_asset_vmr["cod"] + pd.DateOffset(years=20) 
        #Create a column date dementelemnt and set default value as nan
        df_['date_dementelement'] = np.nan
        df_to_asset_vmr['date_dementelement'] = np.nan

        #To create a repowering column
        df_['repowering'] = np.nan
        df_to_asset_vmr['repowering'] = np.nan
        #To create a column en_planif
        df_['en_planif'] = 'Oui'
        df_to_asset_vmr['en_planif'] = 'Non'

        #correct correct "eolien" spelling
        df_["technologie"] = df_["technologie"].str.replace("éolien ", "éolien")
        df_to_asset_vmr["technologie"] = df_to_asset_vmr["technologie"].str.replace("éolien ", "éolien")

        #To create a column id 
        df_ = df_.assign(id=[1 + i for i in xrange(len(df_))])[['id'] + df_.columns.tolist()]
        #To create a column asset_id 
        df_ = df_.assign(asset_id=[(len(df_)+1) + i for i in xrange(len(df_))])[['asset_id'] + df_.columns.tolist()]
        df_to_asset_vmr = df_to_asset_vmr.assign(id=[1 + i for i in xrange(len(df_to_asset_vmr))])[['id'] + df_to_asset_vmr.columns.tolist()]
        df_to_asset_vmr = df_to_asset_vmr.assign(asset_id=[1 + i for i in xrange(len(df_to_asset_vmr))])[['asset_id'] + df_to_asset_vmr.columns.tolist()]

        #To select only specific rows
        df_asset_planif = df_[['id', "asset_id", 'projet_id', 'projet', 'technologie', 
                              'cod', 'mw', 'taux_succès', 'puissance_installée', 'eoh', 
                              'date_merchant', 'date_dementelement', 'repowering', 
                               'date_msi', 'en_planif']]

        #To select only specific rows(df containing hedge template data of projects in planification)
        hedge_planif = df_[["id", "projet_id", "projet", "technologie", "cod", 
                            "date_merchant", "date_dementelement", 
                            "puissance_installée", "en_planif"]]
        hedge_planif = hedge_planif.assign(hedge_id=[(len(hedge_planif)+1) + i for i in xrange(len(hedge_planif))])[['hedge_id'] + hedge_planif.columns.tolist()]
        hedge_planif=hedge_planif.assign(id=[1 + i for i in xrange(len(hedge_planif))])[['id'] + hedge_planif.columns.tolist()]
        #Select only specific columns 
        df_to_asset_vmr = df_to_asset_vmr[['id', 'projet_id', 'projet', 'technologie', 'cod', 'mw', 'taux_succès', 
                                         'puissance_installée', 'eoh', 'date_merchant', 'date_dementelement', 
                                         'repowering', 'date_msi', 'en_planif']]
        #To export data as excel files
        df_to_asset_vmr.to_excel(temp_dir+'planif_to_asset_vmr.xlsx', index=False, float_format="%.3f")#This file contains data of assets already in production but were in planification
        hedge_planif.to_excel(temp_dir+"hedge_planif.xlsx", index=False, float_format="%.3f")#This file contains data tha will be used to create hedge template of assets that are in planification
        #==============================================================================
        #===================== Joining Asset VMR and Asset Planif =====================
        #==============================================================================
        #To join the 2 partial template
        frames = [df_asset_vmr, df_asset_planif]
        asset_vmr_planif = pd.concat(frames)
        asset_vmr_planif.reset_index(inplace=True, drop=True)

        #To create a column containing row number
        asset_vmr_planif.drop("id", axis=1, inplace=True)
        template_asset_without_prod=asset_vmr_planif.assign(id=[1 + i for i in xrange(len(asset_vmr_planif))])[['id'] + asset_vmr_planif.columns.tolist()]
        print('template asset ends!:\n')
        return template_asset_without_prod
    except Exception as e:
        print("Template asset transformation error!: "+str(e))
        
        
def Load(dest_dir, src_flow, file_name, file_extension):
    try:
        if file_extension in ['.xlsx', '.xls', '.xlsm', '.xlsb', '.odf', '.ods', '.odt']:
            src_flow.to_excel(dest_dir+file_name+file_extension, index=False, float_format="%.4f")
        else: 
            src_flow.to_csv(dest_dir+file_name+file_extension, index=False, float_format="%.4f", encoding='utf-8-sig')
        print("Data loaded succesfully!")
    except Exception as e:
        print("Data load error!: "+str(e))
        

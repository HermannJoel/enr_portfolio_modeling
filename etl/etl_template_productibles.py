import pandas as pd
import sys
import xlsxwriter
import os
import configparser
from datetime import datetime
# adding etls/functions to the system path
sys.path.insert(0, 'D:/git-local-cwd/Data-Engineering-Projects/blx_mdp_data-eng/etls/functions')
from etl_functions import (RemoveP50P90TypeHedge, CreateDataFrame, 
                           MergeDataFrame, AdjustedByPct, ChooseCwd,
                           RemoveP50P90, ReadExcelFile, SelectColumns,CreateMiniDataFrame)

#Load Config
config_file=os.path.join(os.path.dirname("__file__"), 'config/config.ini') 
config=configparser.ConfigParser(allow_no_value=True)
config.read(config_file)

dest_dir=os.path.join(os.path.dirname("__file__"), config['develop']['dest_dir'])
temp_dir=os.path.join(os.path.dirname("__file__"),config['develop']['temp_dir'])


def Extract(productible_path, project_names_path, template_asset_path):
    ''' Function to extract excel files.
    Parameters
    ==========
    productible_path: str
        path excel file containing data hedge in prod
    project_names_path: str
        path excel file containing data hedge in prod
    template_asset_path: str
        path excel file containing data hedge in prod
    Returns
    =======
    df_productible: DataFrame
        asset productibles dataframe
    df_profile: DataFrame
        asset productibles profile dataframe
    df_project_names: DataFrame
        
    df_template_asset: DataFrame
        data template asset w/o productibles
    '''
    try:
        df_productibles=ReadExcelFile(productible_path, sheet_name="Budget 2022", header=1)
        df_profile=ReadExcelFile(productible_path, sheet_name="BP2022 - Distribution mensuelle", header=1)
        df_project_names=ReadExcelFile(project_names_path)
        df_template_asset=ReadExcelFile(template_asset_path)
        
        return df_productibles, df_profile, df_project_names, df_template_asset
    except Exception as e:
        print("Data Extraction error!: "+str(e))


def Transform(data_productible, data_profile, data_project_names, data_template_asset):
    try:
        #To import prod data from as pd data frame  
        df = data_productible
        df = df[["Projet", "Budget 2022 (KWh) - P50", "Budget 2022 (KWh) - P90 "]]
        df = df.iloc[0:105,:]#Eventualy this can change if new parcs are added. 0:105 because 105 is the last row containing prod data
        #Divide prod by 1000 to convert in MWH 
        df[["Budget 2022 (KWh) - P50", "Budget 2022 (KWh) - P90 "]] = df[["Budget 2022 (KWh) - P50", "Budget 2022 (KWh) - P90 "]]/1000
        df.columns = ["projet", "p50", "p90"]
        #To create a list containing projects that are not longer in production (dismantled or sold)
        out_projets = ["Cham Longe Le Courbil (Eole Cevennes)", "Cham Longe Bel Air", 
                       "La Bouleste", "CDB Doux le vent","Evits et Josaphats", "Remise Reclainville", 
                       "Bougainville", "Renardières mont de Bezard","Blendecques Elec", "Stockage de l'Arce"]
        
        #Drop rows that contain any value in the list and reset index
        df_productibles_ = df[df.projet.isin(out_projets) == False]
        df_productibles_.sort_values(by=['projet'], inplace=True, ignore_index=True)
        df_productibles_.reset_index(inplace=True, drop=True)
        

        df_ =data_profile
        df_ = df_.iloc[0:12, 2:108]#This may change
        df_.rename(columns = {'% du P50':'month'}, inplace=True)

        #To create a list containing projects that are not longer in production (dismantled or sold)
        out_projets_ = ["Cham Longe Le Courbil (Eole Cevennes)", "Cham Longe Bel Air", 
                        "La Bouleste", "CDB Doux le vent", "Evits et Josaphats", 
                        "Remise Reclainville", "Bougainville", "Renardières mont de Bezard", 
                        "Blendecques Elec"]
        #Drop project taht are in the list above
        df_.drop(out_projets_, axis=1, inplace=True)

        #To create a list containing solar parcs
        solaire = ["Boralex Solaire Les Cigalettes SAS (Montfort)", 
                   "Boralex Solaire Lauragais SAS",
                   "Saint Christophe (Clé des champs)", 
                   "Peyrolles"]
        #To calculate typical solar profil as the mean of solar profil
        df_["m_pct_solaire"] = df_.loc[:,solaire].mean(axis=1)
        ##To calculate typical wind power profil as the mean of wind power profil
        df_["m_pct_eolien"] = df_.iloc[:,1:].drop(solaire, axis=1).mean(axis=1)

        #To create a df containing   
        mean_profile = df_.iloc[:,[0,-2,-1]]
        profile = df_.iloc[:, 1:-2]
        if set(["Extension seuil de Bapaume XSB","Extension plaine d'Escrebieux XPE"]).issubset(profile.columns):
            try:
                #To rename (add parentheses) on projet names
                profile.rename(columns = {'Extension seuil de Bapaume XSB':'Extension seuil de Bapaume (XSB)'}, inplace=True)
                profile.rename(columns = {"Extension plaine d'Escrebieux XPE":"Extension plaine d'Escrebieux (XPE)"}, inplace=True)
            except:
                print('the columns are not in the profile data frame!')
        else:
            profile.columns=profile.columns
        
        #To join 2 data frame
        frames = [data_project_names, df_productibles_]
        df__ = pd.concat(frames, axis=1, ignore_index=False)
        #To create a new column with projet_id
        n = 3
        df__.loc[df__['projet'].str[:n] == df__['projet_name'].str[:n], 'projet_id'] = df__["code"]
        df_productibles__=df__[["projet_id", "projet", "p50", "p90"]]
        
        #To change prod_perc column names by projet_id
        n = 5
        s = (df__.assign(names=df__['projet'].str[:n])
             .drop_duplicates('names')
             .set_index('names')['projet_id']
            )

        profile_id = (pd
                      .concat([profile.columns.to_frame().T, profile])
                      .rename(columns=lambda x: s.loc[x[:n]])
                  )

        profile_id.reset_index(inplace=True, drop=True)
        profile_id=profile_id.iloc[1:,:]
        
        template_asset_with_prod = pd.merge(data_template_asset, df_productibles__, how="left", on=['projet_id', 'projet'])
        
        return df_productibles__, profile_id, profile, mean_profile, template_asset_with_prod
    
    except Exception as e:
        print("Template hedge transformation error!: "+str(e))
    
def Load(dest_dir, src_productible, src_profile_id, src_profile, src_mean_profile, file_name):
    try:
        #To export prod with no projet_id, profil with no projet_id, typical profil data as one excel file 
        #Create a Pandas Excel writer using XlsxWriter as the engine.
        writer = pd.ExcelWriter(dest_dir+file_name+'.xlsx', engine='xlsxwriter')
        #Write each dataframe to a different worksheet.
        src_productible.to_excel(writer, sheet_name="productible", float_format="%.4f", index=False)
        src_profile_id.to_excel(writer, sheet_name="profile_id", float_format="%.4f", index=False)
        src_profile.to_excel(writer, sheet_name="profile", float_format="%.4f", index=False)
        src_mean_profile.to_excel(writer, sheet_name="mean_profile", float_format="%.4f", index=False)
        #Close the Pandas Excel writer and output the Excel file.
        writer.save()
        print("Data loaded succesfully!")
    except Exception as e:
        print("Data load error!: "+str(e))


def LoadTemplateAsset(dest_dir, src_flow, file_name, file_extension):
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
    >>> to load template_asset_with_prod in dest_dir as template_asset.csv 
    """
    try:
        if file_extension in ['.xlsx', '.xls', '.xlsm', '.xlsb', '.odf', '.ods', '.odt']:
            src_flow.to_excel(dest_dir+file_name+file_extension, index=False, float_format="%.4f")
        else: 
            src_flow.to_csv(dest_dir+file_name+file_extension, index=False, float_format="%.4f", encoding='utf-8-sig')
        print("Template asset w/i prod loaded succesfully!")
    except Exception as e:
        print("Data load error!: "+str(e))

    
import pandas as pd
import os
os.chdir('D:/local-repo-github/enr_portfolio_modeling/')
from src.utils.functions import*

def extract_prod(productible_path, project_names_path, template_asset_path):
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
        df_productibles = read_excel_file(productible_path, sheet_name="Budget 2022", header=1)
        df_profile = read_excel_file(productible_path, sheet_name="BP2022 - Distribution mensuelle", header=1)
        df_project_names = read_excel_file(project_names_path)
        df_template_asset = read_excel_file(template_asset_path)
        
        return df_productibles, df_profile, df_project_names, df_template_asset
    except Exception as e:
        print("Data Extraction error!: "+str(e))
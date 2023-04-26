import pandas as pd
import numpy as np
from datetime import datetime
import sys
import configparser
import os
pd.options.mode.chained_assignment=None
os.chdir('D:/local-repo-github/enr_portfolio_modeling/')
from functions import*

config_file=os.path.join(os.path.dirname("__file__"), 'Config/config.ini') 
config=configparser.ConfigParser(allow_no_value=True)
config.read(config_file)

hedge_vmr = os.path.join(os.path.dirname("__file__"),config['develop']['hedge_vmr'])
hedge_planif = os.path.join(os.path.dirname("__file__"),config['develop']['hedge_planif'])
src_dir = os.path.join(os.path.dirname("__file__"),config['develop']['src_dir'])
dest_dir = os.path.join(os.path.dirname("__file__"),config['develop']['dest_dir'])
temp_dir = os.path.join(os.path.dirname("__file__"),config['develop']['temp_dir'])
google_application_credentials = os.path.join(os.path.dirname("__file__"),config['develop']['google_application_credentials'])
datasetid = os.path.join(os.path.dirname("__file__"),config['develop']['datasetid']) #gbq stg ddb
tableid = os.path.join(os.path.dirname("__file__"),config['develop']['tableid'])
bucketid = os.path.join(os.path.dirname("__file__"),config['develop']['bucketid'])
mssqluid = os.path.join(os.path.dirname("__file__"),config['develop']['mssqluid'])
mssqlserver = os.path.join(os.path.dirname("__file__"),config['develop']['mssqlserver'])
msqsldriver = os.path.join(os.path.dirname("__file__"),config['develop']['mssqlserver'])
mssqldb = os.path.join(os.path.dirname("__file__"),config['develop']['mssqldw'])
uri = os.path.join(os.path.dirname("__file__"),config['develop']['uri'])
mongodbatlas_stg_conn_str = os.path.join(os.path.dirname("__file__"),config['develop']['mongodbatlas_stg_conn_str']) 


def extract_hedge(hedge_vmr_path, hedge_planif_path):
    ''' Function to extract excel files.
    Parameters
    ==========
    hedge_vmr_path: str
        path excel file containing data hedge in prod
    hedge_planif_path: str
        path excel file containing data hedge in planif    
    Returns
    =======
    df_hedge_vmr: DataFrame
        hedge vmr dataframe
    df_hedge_planif: DataFrame
        hedge planif dataframe
    '''
    try:
        df_hedge_vmr = read_excel_file(hedge_vmr_path)
        df_hedge_planif = read_excel_file(hedge_planif_path)
        return df_hedge_vmr, df_hedge_planif 
    except Exception as e:
        print("Data Extraction error!: "+str(e))

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

def load_hedge(dest_dir, src_flow, file_name, file_extension):
    """Function to load data as excle file     
    parameters
    ==========
    dest_dir (str) :
        target folder path
    src_flow (DataFrame) :
        data frame returned by transform function        
    file_name (str) : 
        destination file name
    file_extension (str) :
        file extension as xlsx, csv, txt...
    exemple
    =======
    load(dest_dir, template_asset_without_prod, 'template_asset', '.csv')
    >>>  
    """
    try:
        if file_extension in ['.xlsx', '.xls', '.xlsm', '.xlsb', '.odf', '.ods', '.odt']:
            src_flow.to_excel(dest_dir+file_name+file_extension, index=False, float_format="%.4f")
        else: 
            src_flow.to_csv(dest_dir+file_name+file_extension, index=False, float_format="%.4f", encoding='utf-8-sig')
        print(f"Data loaded in {dest_dir} as csv!")
    except Exception as e:
        print(f"Data load as {file_name}.{file_extension} error!: "+str(e))
        
    
if __name__ == '__main__':
    df_hedge_vmr, df_hedge_planif = extract(hedge_vmr_path=hedge_vmr, hedge_planif_path=hedge_planif)
    src_hedge = transform(hedge_vmr=df_hedge_vmr, hedge_planif=df_hedge_planif)
    load(dest_dir = dest_dir, src_flow = src_hedge, file_name="template_hedge", file_extension='.csv')
    # load_docs_to_mongodb(dest_db='staging', dest_collection='Hedge', 
    #                      src_data= src_hedge, 
    #                      date_format = '%Y-%m-%d', 
    #                      mongodb_conn_str = mongodbatlas_stg_conn_str)
    
    stg_hedge = read_doc_from_mongodb(src_db = 'staging', src_collection = 'Hedge', 
                          query={}, no_id=True , 
                          mongodb_conn_str = mongodbatlas_stg_conn_str)
    
    load_data_to_mssql(src_data = stg_hedge, dest_table = 'Hedge', 
                             #mssqlserver = mssqlserver, mssqldb = mssqldb,
                             if_exists = 'replace',
                             dtype={
                                    'Id': 'Integer',
                                    'HedgeId': 'Integer',
                                    'ProjetId': 'String(50)',
                                    'Projet': 'String(250)',
                                    'TypeHedge': 'String(50)',
                                    'ContractStartDate': 'Date',
                                    'ContractEndDate': 'Date',
                                    'Profil': 'String(100)', 
                                    'HedgePct': 'DECIMAL(5, 2)', 
                                    'Counterparty': 'String(100)', 
                                    'CountryCounterparty': 'String(100)'
                             }, 
                             schema = 'stg')
    
    load_blob_to_gcs(source_file_name = dest_dir+"template_hedge.csv", 
                     bucket_name = bucketid, destination_blob_name = 'template_hedge.csv',
                     google_application_credentials = google_application_credentials)
    
if __name__ == '__main__':
    load_data_to_gcbq_from_gcs(uri = uri,
                               table_name = tableid,
                               google_application_credentials = google_application_credentials,
                               schema=[
                                   bigquery.SchemaField('Id', 'INTEGER'),
                                   bigquery.SchemaField('HedgeId', 'INTEGER'),
                                   bigquery.SchemaField('ProjetId', 'STRING'),
                                   bigquery.SchemaField('Projet', 'STRING'),
                                   bigquery.SchemaField('TypeHedge', 'STRING'),
                                   bigquery.SchemaField('ContractStartDate', 'DATE'),
                                   bigquery.SchemaField('ContractEndDate', 'DATE'),
                                   bigquery.SchemaField('Profil', 'STRING'),
                                   bigquery.SchemaField('HedgePct', 'NUMERIC'),
                                   bigquery.SchemaField('Counterparty', 'STRING'),
                                   bigquery.SchemaField('CountryCounterparty', 'STRING')
                               ] 
                              )
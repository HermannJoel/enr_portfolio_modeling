import pandas as pd
import numpy as np
from datetime import datetime
xrange = range
import warnings
import os
import pathlib
import psycopg2
import pyodbc
import sqlalchemy as sqlalchemy
from sqlalchemy import create_engine
import urllib
from pymongo import MongoClient
import dash_bootstrap_components as dbc
from dash import html
from google.cloud import bigquery
from google.cloud import storage
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
import csv
from io import StringIO

def remove_p50_p90_type_hedge(data, *args, **kwargs):
    """udf to remove p50 p90 values based on date_debut and date_fin
    condition:The date value is less than date_debut and higher than date_fin 
    Paremeters
    ==========
    *Args:
        data: DataFrame 
        sd: (str) 
            The arg takes the value 'date_debut' 
        ed: (str)
            The arg takes the value 'date_fin'
        p50: (str)
            The arg takes the value 'p50_adj'
        p90: (str) 
            The arg takes the value of the column label 'p90_adj'
        th: (str) 
            The arg takes the value 'type_hedge'
        date: (str) 
            The arg takes the value 'date'
        projetid: (str) 
            The arg takes the value 'projet_id'
        hedgeid: (str)  
            The arg takes the value 'hedge_id'
            
        cond : (condition 1) 'date' column is less (in total seconds) than the given projet_id's first 'date_debut' value 
        cond_2 : (condition 2) 'date' column is higher (in total seconds) than the given projet_id's first 'date_fin' value
    """
    cond=((data[kwargs['date']] - data.groupby([kwargs['projetid'], kwargs['hedgeid']])[kwargs['sd']].transform('first')).dt.total_seconds())<0
    data[kwargs['p50']] = np.where(cond,'', data[kwargs['p50']])
    data[kwargs['p90']] = np.where(cond,'', data[kwargs['p90']])
    #To remove type of hedge based on date cod
    data[kwargs['th']]=np.where(cond,'', data[kwargs['th']])
    #To remove p50 p90 based on date_fin
    cond_2=((data[kwargs['date']] - data.groupby([kwargs['projetid'], kwargs['hedgeid']])[kwargs['ed']].transform('first')).dt.total_seconds())>0
    data[kwargs['p50']] = np.where(cond_2, '', data[kwargs['p50']])
    data[kwargs['p90']] = np.where(cond_2, '', data[kwargs['p90']])
    #To remove type_hedge based on date_fin
    data[kwargs['th']]=np.where(cond_2,'', data[kwargs['th']])
    #To reset index
    data.reset_index(inplace=True, drop=True)
    data=data.assign(id=[1 + i for i in xrange(len(data))])[['id'] + data.columns.tolist()]
    return data

def remove_p50_p90(data, *args, **kwargs):
    """
    To remove p50 p90 values based on date_debut and date_fin
    condition:The date value is prior to date_debut and post to date_fin    
*Args:
    data (DataFrame) :
    cod (str) : The arg takes the value 'cod' 
    dd (str) : The arg takes the value 'date_dementelement'
    p50 (str) : The arg takes the value 'p50_adj'
    p90 (str) : The arg takes the value of the column label 'p90_adj'
    date (str) : The arg takes the value 'date'
    projetid (str) : The arg takes the value 'projet_id'
    assetid (str) :  The arg takes the value 'asset_id'
    
Parameters:
    cond : (condition 1) 'date' column is less (in total seconds) than a given projet_id's first 'date_debut' value 
    cond_2 : (condition 2) 'date' column is higher (in total seconds) than a given projet_id's first 'date_fin' value
    """
    cond=((data[kwargs['date']] - data.groupby(kwargs['projetid'])[kwargs['cod']].transform('first')).dt.total_seconds())<0
    data[kwargs['p50']] = np.where(cond,'', data[kwargs['p50']])
    data[kwargs['p90']] = np.where(cond,'', data[kwargs['p90']])
    #To remove p50 p90 based on date_fin
    cond_2=((data[kwargs['date']] - data.groupby(kwargs['projetid'])[kwargs['dd']].transform('first')).dt.total_seconds())>0
    data[kwargs['p50']] = np.where(cond_2, '', data[kwargs['p50']])
    data[kwargs['p90']] = np.where(cond_2, '', data[kwargs['p90']])
    #To reset index
    data.reset_index(inplace=True, drop=True)
    data=data.assign(id=[1 + i for i in xrange(len(data))])[['id'] + data.columns.tolist()]
    return data

def create_data_frame(data, *args, **kwargs):
    """
    To create a DataFrame containing p50 and P90 across our time horizon     
    args:
    data (DataFrame) :
    
    *args: non-keyworded arguments
        sd (str) : Takes the value of the start of the horizon  dd-mm-yyyy  '01-01-2022'
    **kwargs : keyworded arguments
        a (int) : Takes the value 0
        b (int) : Takes the value of the length of our horizon (12*7)
        profile (dictionaries) : The arg takes the value of the production profile
        n (int) : The arg takes the value length of data 
        date (str) : The arg takes the value of date colum label 'date'
    """
    pd.options.display.float_format = '{:.5f}'.format
    start_date=pd.to_datetime(args*kwargs['n'])
    d=pd.DataFrame()
    for i in range(kwargs['a'], kwargs['b']):
        data.loc[:, kwargs['date']]=start_date
        list_p50=[]
        list_p90=[]
        for elm in data.projet_id:
            try: 
                list_p50.append(-kwargs['profile'][elm][start_date.month[0]-1]*float(data[data.projet_id==elm]["p50"].values[0]))
            except:
                list_p50.append("NA")
            try:
                list_p90.append(-kwargs['profile'][elm][start_date.month[0]-1]*float(data[data.projet_id==elm]["p90"].values[0]))
            except:
                list_p90.append("NA")
        data["p50_adj"]=list_p50
        data["p90_adj"]=list_p90
        d=pd.concat([d, data],axis=0)
        start_date=start_date + pd.DateOffset(months=1) 
    return d 


def adjusted_by_pct(data, **kwargs):
    """
    To compute adjusted p50 & p90 by hedge percentage (pct_couverture)    
    Args:
    data (DataFrame) :
    col1 (str) : Takes the value p50_adj column label
    col2 (str) : Takes the value of pct_couverture column label
    """   
    return round(data[kwargs['col1']].apply(lambda x: float(x)), 4) * round(data[kwargs['col2']].apply(lambda x: float(x)), 4)

def merge_data_frame(*args):
    """To merge df 
    Parameters
    ==========
    * : DataFrame, 
    """
    frames=args
    merged_df=pd.concat(frames)
    merged_df.reset_index(drop=True, inplace=True)
    return  merged_df


def choose_cwd(**kwargs):
    try:
        os.chdir(kwargs['cwd'])
        print('the working directory has been changed!') 
        print('cwd: %s ' % os.getcwd())
    except NotADirectoryError():
        print('you have not chosen directory!')
    except FileNotFoundError():
        print('the folder was not found. the path is incorect!')
    except PermissionError():
        print('you do not have access to this folder/file!')
        

def read_excel_file(path, **kwargs):
    ext=pathlib.Path(path).suffix
    if ext in ['.xlsx', '.xls', '.xlsm', '.xlsb', '.odf', '.ods', '.odt']:
        return pd.read_excel(path, **kwargs)
    else: 
        return pd.read_csv(path, **kwargs)

    
def format_float(df, column, decimals=2):
    df[column] = df[column].apply(lambda x: f"{x:,.{decimals}f}")
    return df
 
def select_columns(data, *args):
    columns=args
    list=[]
    for i in columns:
        list.append(i)
    selection=data[list]
    return selection

def create_mini_data_frame(data, *args, **kwargs):
    """To create a DataFrame containing p50 and P90 across our time horizon     
    Parameters
    ==========
    data : DataFrame,
    * 
    sd : str, 
        Takes the value of the start of the horizon  dd-mm-yyyy  '01-01-2022'
    **
    a : int, 
        Takes the value 0
    b : int,
        Takes the value of the length of our horizon (12*7)
    n : int,
        The arg takes the value length of data 
    date : str,
        The arg takes the value of date colum label 'date'
    """
    start_date=pd.to_datetime(args*kwargs['n'])
    d=pd.DataFrame()
    for i in range(kwargs['a'], kwargs['b']):
        data.loc[:, kwargs['date']]=start_date
        d=pd.concat([d, data],axis=0)
        start_date=start_date + pd.DateOffset(months=1)
    return d


def dis_warn():
    warnings.warn("deprecated", DeprecationWarning)

    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        
    
def remove_contract_prices(data, *args, **kwargs):
    """To remove Contract prices values based on date_debut and date_fin
    condition:The date value is prior to date_debut and post to date_fin 
    
    Parameters
    ==========
    data (DataFrame) :
            
    sd : str,
            The arg takes the value 'date_debut' 
    ed : str,
            The arg takes the value 'date_fin'
    dd : str,
            The arg takes the value 'date_dementelement'
    price : str,
            The arg takes the value 'contract_price'
    date : str,
            The arg takes the value 'date'
    projetid : str,
            The arg takes the value 'projet_id'
    hedgeid : str,  
            The arg takes the value 'hedge_id'
    th : str,
            Type hedge   
    cond : 'date' column is less (in total seconds) than a given projet_id's first 'date_debut' value 
    cond_2 : 'date' column is higher (in total seconds) than a given projet_id's first 'date_fin' value
    cond_3 : 'date' column is higher (in total seconds) than a given projet_id's first 'date_dementelement' value
    """
    cond=((data[kwargs['date']] - data.groupby([kwargs['projetid'], kwargs['hedgeid']])[kwargs['sd']].transform('first')).dt.total_seconds())<0
    #To remove prices based on date_debu
    data[kwargs['price']] = np.where(cond,'', data[kwargs['price']])
    #To remove based on date_fin
    cond_2=((data[kwargs['date']] - data.groupby([kwargs['projetid'], kwargs['hedgeid']])[kwargs['ed']].transform('first')).dt.total_seconds())>0
    data[kwargs['price']] = np.where(cond_2, '', data[kwargs['price']])
    #To remove prices based on date_dementelement
    cond_3=((data[kwargs['date']] - data.groupby([kwargs['projetid'], kwargs['hedgeid']])[kwargs['dd']].transform('first')).dt.total_seconds())>0
    data[kwargs['price']] = np.where(cond_3, '', data[kwargs['price']])   
    #To remove type of hedge based on date_debut
    data[kwargs['th']]=np.where(cond,'', data[kwargs['th']])
    #To remove type of hedge based on date_fin
    data[kwargs['th']]=np.where(cond_2,'', data[kwargs['th']])
    #To remove type of hedge based on date_dementelement
    data[kwargs['th']]=np.where(cond_3,'', data[kwargs['th']])
    #To reset index
    data.reset_index(inplace=True, drop=True)
    data=data.assign(id=[1 + i for i in xrange(len(data))])[['id'] + data.columns.tolist()]
    return data


def extract_data_from_excel_file():
    try:
        directory=dir
        for filename in os.listdir(dir):
            files_wo_ext=os.path.splitext(filename)[0]
            if filename.endswith(".xlsx"):
                f=os.path.join(directory, filename)
                if os.path.isfile(f):
                    df=pd.read_excel(f)
    except Exception as e:
        eml.send_email(to, "File upload, data extract error!: ", f"Data Extract Error: File location {dir}" + str(e))
        print("Data Extract error!: "+str(e))
        
"""
from email.mine.text import MIMEText
from email.mine.application import MIMEApplication
from email.mine.multipart import MIMEMultipart
from email.mine.base import MIMEBase
from email.message import EmailMessage
from email import encoders
import smtplib

sender=hermannjoel.ngayap@yahoo.fr
to
"""
        
        
def send_email(to, subject, content):
    message=MIMEMultipart()
    message["Subject"]=subject
    message["From"]=sender
    message["To"]=to
    
    body_content=content
    message.attach(MiMEText(body_content, "html"))
    msg_body=message.as_string()
    
    server=smtplib.SMTP("localhost")
    server.login(email, password)
    server.sendmail(sender, to, msg_body)
    
    server.quit()
    


def open_postgres_db():
    print('Connecting to db!')
    connection_string = f"postgresql://{username}:{pwd}@{hostname}:{portid}/{database}"
    cnxn = psycopg2.connect(connection_string)
    cursor = cnxn.cursor()
    print( "Connected!\n")
    return cnxn
    
def query_data_from_postgreSQL(query, db_connection_string):
    engine = sqlalchemy.create_engine(db_connection_string)
    connection = engine.connect()
    dataframe= pd.read_sql_query(
        sql=query, con=db_connection_string
    )
    return dataframe

def read_data_from_mssql(query):
    cnxn = pyodbc.connect("Driver={ODBC Driver 17 for SQL Server};"
                          "Server=DESKTOP-JDQLDT1/MSSQLSERVERDWH;"
                          "Database=DWH;"
                          "Trusted_Connection=yes;")
    cursor = cnxn.cursor()
    dataframe= pd.read_sql_query(
        sql=query, con=cnxn
    )
    return dataframe

def read_from_postgresql(query, connection_string):
    cnxn = psycopg2.connect(connection_string)
    cursor = cnxn.cursor()
    dataframe= pd.read_sql_query(
        sql=query, con=connection_string
    )
    return dataframe


def insert_in_sql(dataset, table, conn):
    engine = sqlalchemy.create_engine(conn)
    connection = engine.connect()
    dataset.to_sql(name = table, con = connection, if_exists = 'append', index = False)

def date_convert(date_col_to_convert):
    #return list(map(lambda x: datetime.datetime.strptime(x,'%b %d, %Y').strftime('%Y-%m-%d'), old_df['oldDate']))
    return datetime.datetime.strptime(date_col_to_convert, '%b %d, %Y').strftime('%Y-%m-%d')
    

def convert_date_columns(dataframe, date_format):
    for col in dataframe.columns:
        if dataframe[col].dtype == 'datetime64[ns]' or 'date' in col.lower():
            try:
                #dataframe[col] = pd.to_datetime(dataframe[col], format=date_format)
                dataframe[col] = dataframe[col].apply(lambda x: datetime.strptime(x, date_format))
            except:
                continue
    return dataframe
  
    
def load_postgres_docs_to_mongodb(dest_db, dest_collection, query_name, **kwargs):
    try:
        myclient=MongoClient(kwargs['mongodb_conn_str'])
        db=myclient[dest_db]
        collection=db[dest_collection]
        collection.insert_many(
            convert_date_columns(
                query_data_from_postgreSQL(
                    query=query_name, db_connection_string=kwargs['postgres_conn_str']), kwargs['date_format']
            ).to_dict('records')
        )
        print("Data imported in mondgobd successfully!")
    except Exception as e:
        print("Data Import error!: "+str(e))
        

def load_config_file(config_file_path):
    #Load Config
    config_file=os.path.join(os.path.dirname("__file__"), config_file_path) 
    config=configparser.ConfigParser(allow_no_value=True)
    config.read(config_file)
    

def make_dbc_table(df, style={}):
    """Make a dbc table from a pandas DataFrame df."""
    table_header = [html.Thead(html.Tr([html.Th(col) for col in df.columns]))]
    table_body = [
        html.Tbody(
            [
                html.Tr([html.Td(df.iloc[i][col]) for col in df.columns])
                for i in range(len(df))
            ]
        )
    ]
    return dbc.Table(
        table_header + table_body,
        bordered=False,
        responsive=True,
        hover=True,
        striped=True,
        style=style,
        class_name="table",
    )
        
def format_float(func, *args):
    result=func(*args)
    return round(result)

def load_pickle(filename):
    unpickleFile = open(filename, 'rb')
    file = pickle.load(unpickleFile)
    return file 

def rename_df_columns(df: pd.DataFrame, column_names: list):
    """Rename DataFrame columns with strings from list"""
    for i, col in enumerate(df.columns):
        df = df.rename(columns={col: column_names[i]})
    return df

def read_docs_from_mongodb(src_db, src_collection, column_names, query={}, no_id=True , **kwargs):
    try:
        myclient = MongoClient(kwargs['mongodb_conn_str'])
        db = myclient[src_db]
        collection = db[src_collection]
        cursor = collection.find(query)
        data_frame = pd.DataFrame(list(cursor))
        # Delete the _id
        if no_id:
            del data_frame['_id']
        data_frame = rename_df_columns(data_frame, column_names)
        return data_frame
        print(f"Data exported from {src_db}.{src_collection} successfully!")
    except Exception as e:
        print(f"Data read from {src_db}.{src_collection} error!: "+str(e))
        
def load_as_excel_file(dest_dir, src_flow, file_name, file_extension):
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
    load_as_excel_file(dest_dir, template_asset_without_prod, 'template_asset', '.csv')
    >>>  
    """
    try:
        if file_extension in ['.xlsx', '.xls', '.xlsm', '.xlsb', '.odf', '.ods', '.odt']:
            src_flow.to_excel(dest_dir+file_name+file_extension, index=False, float_format="%.4f")
        else: 
            src_flow.to_csv(dest_dir+file_name+file_extension, index=False, float_format="%.4f", encoding='utf-8-sig')
            print(f"Data loaded in {dest_dir} as {file_name}{file_extension}!")
    except Exception as e:
        print(f"Data load as {file_name}.{file_extension} error!: "+str(e))

def load_data_in_postgres_table(src_data, dest_table, pguid, pgpass, pgserver, pgdb, **kwargs):
    try:
        engine = create_engine(f"postgresql+psycopg2://{pguid}:{pgpass}@{pgserver}:5432/{pgdb}")
        src_data.to_sql(dest_table, kwargs['schema'], con=engine,
                        index=False, if_exists='replace',
                        chunksize=1000,
                       )
        print(f"Data inserted in postgres db:{pgdb} successfully")
    except Exception as e:
        print(f"Data load in {kwargs['schema']}.{dest_table} error!:" +str(e))
        
def load_data_to_mssql(src_data, dest_table, mssqlserver, mssqldb='DWH', dtype={}, yes='yes',**kwargs):
    """Function to load data in mssql db     
    parameters
    ==========
    src_data (str) :
        Source data
    dest_table (str) :
        Destination table in         
    mssqlserver (str) : 
        mssql instance server name
    mssqldb (str) :
        data base name
    dtype={} (Dictionnary) : 
        Dictonnary containing source data type
    **kwargs
    exemple
    =======
    load_data_to_mssql(src_data = src_data, dest_table = 'Hedge', mssqlserver = mssqlserver, mssqldb = mssqldb, if_exists = 'replace', schema = 'stg')
    >>>  
    """
    try:
        cnxn = pyodbc.connect('DRIVER=SQL Server Native Client 11.0'+';SERVER=' + mssqlserver + ';DATABASE=' +mssqldb + ';Trusted_Connection=' +yes)
        cursor = cnxn.cursor()
        cols = ",".join([str(i) for i in src_data.columns.tolist()])
        for i, row in src_data.iterrows():
            sql = "INSERT INTO " +kwargs['schema']+'.'+ dest_table+ " (" +cols + ") VALUES (" + "?,"*(len(row)-1) + "?)"
            cursor.execute(sql, tuple(row))
            cnxn.commit()
        print(f"Data inserted in {mssqldb}.{kwargs['schema']}.{dest_table}!") 
    except Exception as e:
        print(f"Data Import to mssql {mssqldb}.{kwargs['schema']}.{dest_table}. error!: " + str(e))
    finally:
        cursor.close()
        
def rename_df_columns(df: pd.DataFrame, column_names: list):
    """Rename DataFrame columns with strings from list"""
    for i, col in enumerate(df.columns):
        df = df.rename(columns={col: column_names[i]})
    return df
        

def load_docs_to_mongodb(dest_db, dest_collection, src_data, **kwargs):
    try:
        myclient=MongoClient(kwargs['mongodb_conn_str'])
        db=myclient[dest_db]
        collection=db[dest_collection]
        collection.remove({})
        collection.insert_many(
            convert_date_columns(
                src_data
                , kwargs['date_format']
            ).to_dict('records')
        )
        print(f"Data loaded to {dest_db}.{dest_collection} successfully!")
    except Exception as e:
        print(f"Data loaded to {dest_db}.{dest_collection} error!: "+str(e))
        
        
def load_data_to_gcbq_from_gcs(uri, write_disposition=None, schema=[], **kwargs):
    """Function exctract blob from gcs and load to gcbq     
    parameters
    ==========
    uri (str) :
        The path to your file blob to upload
    schema (list) :
        Schema of dest table in dest dataset in gbq 
    **kwargs
    google_application_credentials (str) : 
        storage-object-name, The ID of your GCS object
    datasetid (str) : 
    tableid (str) :
        
    exemple
    =======
    load_blob_to_gcs(source_file_name, bucket_name, destination_blob_name, **kwargs)
    >>>  
    """
    try:
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = kwargs['google_application_credentials'] 
        client = bigquery.Client()
        #dataset_id =  client.dataset(kwargs['dataset_name'])             #db name
        #table_id = dataset_id.table(kwargs['table_name'])                #table name
        table_id = kwargs['table_name']
        job_config = bigquery.LoadJobConfig(
            skip_leading_rows=1, 
            autodetect=True, 
            schema= schema,
            write_disposition = write_disposition,
            ignore_unknown_values=True,
        source_format=bigquery.SourceFormat.CSV,
        )
        uri = uri 
    
        load_job = client.load_table_from_uri(
            uri,
            table_id,
            location="europe-west9",
            job_config=job_config,
        )  
        load_job.result()  
        destination_table = client.get_table(table_id)
        print("Loaded {} rows.".format(destination_table.num_rows)) 
        print(f"Data loaded to {kwargs['table_name']}")
    except Exception as e:
        print(f"Data load to {kwargs['table_name']} error!: "+str(e))
    
def load_blob_to_gcs(source_file_name, bucket_name, destination_blob_name, **kwargs):
    """Function to load blob to glogle storage bucket     
    parameters
    ==========
    source_file_name (str) :
        The path to your file to upload, "local/path/to/file"
    bucket_name (str) :
        Your-bucket-name, The ID of your GCS bucket         
    destination_blob_name (str) : 
        storage-object-name, The ID of your GCS object
    **kwargs
    exemple
    =======
    load_blob_to_gcs(source_file_name, bucket_name, destination_blob_name, **kwargs)
    >>>  
    """
    try: 
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = kwargs['google_application_credentials']
        storage_client = storage.Client() 
        bucket = storage_client.bucket(bucket_name) 
        blob = bucket.blob(destination_blob_name) 
        blob.upload_from_filename(source_file_name) 
        print( f"File {source_file_name} uploaded as {destination_blob_name} to {bucket_name}.") 
    except Exception as e:
        print(f"{destination_blob_name} load to {bucket_name} as {bucket} error!: " + str(e))

        
def load_data_to_mssqlserver(src_data, dest_table, mssqlserver, mssqldb='DWH', dtype={}, yes='yes',**kwargs):
    """Function to load data in mssql db     
    parameters
    ==========
    src_data (str) :
        Source data
    dest_table (str) :
        Destination table in         
    mssqlserver (str) : 
        mssql instance server name
    mssqldb (str) :
        data base name
    dtype={} (Dictionnary) : 
        Dictonnary containing source data type
    **kwargs
    exemple
    =======
    load_data_to_mssql(src_data, dest_table, msqsldriver, mssqlserver, mssqldb, mssqluid, yes=yes, dtype={}, **kwargs)
    >>>  
    """
    try:
        connect_string = urllib.parse.quote_plus(f'DRIVER={{ODBC Driver 17 for SQL Server}};Server=DESKTOP-JDQLDT1\MSSQLSERVERDWH,1433;Database=DWH')
        engine = sqlalchemy.create_engine(f'mssql+pyodbc:///?odbc_connect={connect_string}', fast_executemany=True)
        src_data.to_sql(dest_table, kwargs['schema'], con=engine,
                        index=False, if_exists = kwargs['if_exists'], 
                        chunksize=1000, dtype=dtype, insert_many=True)
        print(f"Data inserted in {mssqldb}.{kwargs['schema']}.{dest_table}!") 
    except Exception as e:
        print(f"Data Import to mssql {mssqldb}.{kwargs['schema']}.{dest_table}. error!: " + str(e))

def update_data_types(data_frame: pd.DataFrame, data_type_dict: dict):
    for col, data_type in data_type_dict.items():
        if col in data_frame.columns:
            data_frame[col] = data_frame[col].astype(data_type)
    return data_frame
        
def read_blob_from_gcs(bucket_name, blob_name, **kwargs):
    try:
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = kwargs['google_application_credentials']
        storage_client = storage.Client() 
        bucket = storage_client.bucket(bucket_name) 
        blob = bucket.blob(blob_name) 
        blob = blob.download_as_text(encoding="utf-8")
        with blob.open("r") as f:
            print('f')        
    except Exception as e:
        print('')
    
        
def load_data_to_snowflake(snowflakeuser:str, gcs_stg_url:str,
                           snowflakepassword:str, snowflakeaccount:str, sf_dest_table:str,
                           gcs_stage:str, snowflakewarehouse:str, snowflakeschema:str, query:str):
    """Function to load data from gcs stage in snowflake to snowflake table
    parameters
    ==========
    snowflakeuser (str) :
        sf User name
    gcs_stg_url (str) :
        url of gcs file to stage in snowflake         
    snowflakepassword (str) : 
        sf password
    snowflakeaccount (str) :
        sf account
    sf_dest_table (str) : 
        Destination table in snowflake
    gcs_stage (str) :
        Stage name in snowflake particulat schema
    snowflakewarehouse (str) :
        Snowflake warehouse name
    snowflakeschema (str) :
        Dest table schame in snowflake
    query (str) :
        Query to copy data from stage and load to sf dest table
    exemple
    =======
    load_data_to_snowflake(snowflakeuser=snowflake_user, gcs_stg_url=gcs_stg_url,
                           snowflakepassword=snowflake_password, snowflakeaccount=snowflake_account, 
                           snowflakewarehouse=snowflake_warehouse, snowflakeschema=snowflake_schema,
                           sf_dest_table = sf_dest_table, gcs_stage = gcs_stage, 
                           query=f"COPY INTO {snowflake_schema}.{sf_dest_table} FROM @{snowflake_schema}.{gcs_stage} FILE_FORMAT = (FORMAT_NAME=MY_FILE_FORMAT) ON_ERROR='ABORT_STATEMENT';")
    >>>  
    """
    try : 
        ctx = snowflake.connector.connect(
            user = snowflakeuser,
            password = snowflakepassword,
            account = snowflakeaccount,
            warehouse = snowflakewarehouse 
        )
        cursor = ctx.cursor()
        cursor.execute(f"USE {snowflakewarehouse};")
        try:
            cursor.execute(f"ALTER WAREHOUSE {snowflakewarehouse} RESUME;")
        except:
            pass
        cursor.execute(f"USE {snowflakeschema};")
        cursor.execute(f"CREATE OR REPLACE FILE FORMAT {snowflakschema}.MY_CSV_FORMAT \
                       TYPE = CSV \
                       FIELD_DELIMITER= ';' \
                       SKIP_HEADER = 1 \
                       NULL_IF = ('NULL', 'null') \
                       EMPTY_FIELD_AS_NULL = true;"
                      )
        cursor.execute(f"CREATE OR REPLACE STAGE {snowflakschema}.{gcs_stage} STORAGE_INTEGRATION = GCP_INT \
                       URL = {gcs_stg_url} \
                       FILE_FORMAT = MY_CSV_FORMAT;"
                      )
        cursor.execute(query)
        print(f"File {gcs_stg_url} loaded to {snowflakewarehouse}.{snowflakeschema}.{sf_dest_table}!")
    except Exception as e:
        print(f"File {gcs_stg_url} load to {snowflakewarehouse}.{snowflakeschema}.{sf_dest_table} error:" + str(e))
    finally:
        cursor.close()
        print('connection closed!')
        

        
        





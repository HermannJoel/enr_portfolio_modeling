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
        
def insert_in_sql(dataset, table, conn):
    engine = sqlalchemy.create_engine(conn)
    connection = engine.connect()
    dataset.to_sql(name = table, con = connection, if_exists = 'append', index = False)
    
    
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
    
    
def load_pickle(filename):
    unpickleFile = open(filename, 'rb')
    file = pickle.load(unpickleFile)
    return file 


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


def excucute_sqlserver_crud_ops(queries:list, mssqlserver:str, mssqldb:str, yes='yes'):
    """Funtion to execute sql server CUD (CREATE, UPDATE, DELETE) operations
    parameters
    ==========
    queries (list) :
        list of queries to execute
    mssqlserver (str) :
        sql server instance name
    mssqldb (str) :
        sql server db name
    yes (str) :
        trusted connection
    exemple
    =======
    excucute_sqlserver_crud_ops(
    queries=[
        "USE ODS",
        "TRUNCATE TABLE ods.Hedge",
        '''INSERT INTO ods.Hedge(HedgeId, ProjetId, Projet, TypeHedge, ContractStartDate, ContractEndDate, Profil, HedgePct, Counterparty, CountryCounterparty) 
        VALUES (393, 'RON3', 'Ronchois 3', 'PPA', '2024-01-01', '2024-06-30', 'As Produced', 1, 'Axpo', 'France'), 
        (392, 'RON3', 'Ronchois 3', 'PPA', '2023-01-01', '2023-06-30', 'As Produced', 0.75, 'Axpo', 'France')'''
    ], 
    mssqlserver='instance_name', 
    mssqldb='db')
    """
    try:
        cnxn = pyodbc.connect('DRIVER=SQL Server Native Client 11.0'+';SERVER=' + mssqlserver + ';DATABASE=' +mssqldb +';Trusted_Connection=' +yes)
        cursor = cnxn.cursor()
        for query in queries:
            try:
                cursor.execute(query)
                cnxn.commit()
                print(f"Query executed successfully: {query}")
            except pyodbc.Error as e:
                print(f"An error occurred while executing query: {query}")
                print(f"Error details: {str(e)}") 
    except pyodbc.Error as e:
        print(f"An error occurred: {str(e)}")
    finally:
        if cursor:
            cursor.close()
        if cnxn:
            cnxn.close()
        print(f"Connection closed!")
        
        
def mongodb_crud_ops(mongodb_db:str, mongodb_collection:str, queries:list, **kwargs):
    """Funtion to execute mongodb CRUD (CREATE, UPDATE, DELETE) operations
    parameters
    ==========
    queries (list) :
        list of queries to execute
    queries = [
    'create_db mydatabase',
    'create_collection mycollection',
    'drop_db mydatabase',
    'drop_collection mycollection',
    'read_collection mycollection'
    ] 
    mongodb_db (str) :
        mongodb database name
    mongodb_collection (str) :
        mongo db collection name
        
    exemple
    mongodb_crud_ops(mongodb_db, mongodb_collection,queries={}, **kwargs)
    >>>
    =======
    """
    try:
        myclient=MongoClient(kwargs['mongodb_cluster_conn'])
        db=myclient[mongodb_db]
        collection=db[mongodb_collection]
        for query in queries:
            command, args = query.split()
            if command == 'create_db':
                try:
                    db_name = args
                    myclient[db_name]
                    print(f"Query executed successfully!: {query}")
                    print(f"Database '{db_name}' created!")
                except Exception as e:
                    print(f"An error occurred while executing query: {query}")
                    print(f"Error details: {str(e)}")
            elif command == 'drop_db':
                try:
                    db_name = args
                    myclient.drop_database(db_name)
                    print(f"Query executed successfully!: {query}")
                    print(f"Database '{db_name}' dropped!")
                except Exception as e:
                    print(f"An error occurred while executing query: {query}")
                    print(f"Error details: {str(e)}")
            elif command == 'create_collection':
                try:
                    db.create_collection(collection_name)
                    print(f"Query executed successfully!: {query}")
                    print(f"Collection '{args}' created.")
                except Exception as e:
                    print(f"An error occurred while executing query: {query}")
                    print(f"Error details: {str(e)}")
            elif command == 'drop_collection':
                try:
                    collection_name = args
                    db.drop_collection(collection_name)
                    print(f"Query executed successfully!: {query}")
                    print(f"Collection '{collection}' dropped.")
                except Exception as e:
                    print(f"An error occurred while executing query: {query}")
                    print(f"Error details: {str(e)}")
            elif command == 'read_collection':
                try:
                    collection_obj = db[collection]
                    docs = list(collection_obj.find())
                    dataFrame = pd.DataFrame(docs)
                    print(f"Query executed successfully!: {query}")
                    return dataFrame
                except Exception as e:
                    print(f"An error occurred while executing query: {query}")
                    print(f"Error details: {str(e)}")
            else:
                print(f"Unknown command: {command}")
    except Exception as e:
        print(f"Mongodb crud operation error!: "+str(e))
    finally:
        if myclient:
            myclient.close()
        print(f"Connection closed!")
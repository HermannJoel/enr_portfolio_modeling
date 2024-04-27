import pandas as pd
import numpy as np

def read_excel_file(path, **kwargs):
    ext=pathlib.Path(path).suffix
    if ext in ['.xlsx', '.xls', '.xlsm', '.xlsb', '.odf', '.ods', '.odt']:
        return pd.read_excel(path, **kwargs)
    else: 
        return pd.read_csv(path, **kwargs)
    
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
        
def query_data_from_postgreSQL(query, db_connection_string):
    engine = sqlalchemy.create_engine(db_connection_string)
    connection = engine.connect()
    dataframe= pd.read_sql_query(
        sql=query, con=db_connection_string
    )
    return dataframe

def read_data_from_mssql(query:str, db:str, server_instance:str, yes='yes'):
    try:
        #cnxn = pyodbc.connect('DRIVER=SQL Server Native Client 11.0'+';SERVER=' + server_instance + ';DATABASE=' +db + ';Trusted_Connection=' +yes)
        # cnxn = pyodbc.connect("Driver={ODBC Driver 17 for SQL Server};"
        #                       "Server=DESKTOP-JDQLDT1/MSSQLSERVERDWH;"
        #                       "Database=ODS;"
        #                       "Trusted_Connection=yes;")
        #cnxn = create_engine(f"mssql+pyodbc://{server_instance}/{db}?driver=SQL+Server+Native+Client+11.0", fast_executemany=True)
        cnxn_str = ("Driver={SQL Server Native Client 11.0};"
            f"Server={server_instance};"
            f"Database={db};"
            f"Trusted_Connection={yes};")
        cnxn = pyodbc.connect(cnxn_str)
        cursor = cnxn.cursor()
        dataFrame= pd.read_sql_query(
            sql=query, con=cnxn
        )
        print(f"Query executed successfully: {query}")
        return dataFrame
    except pyodbc.Error as e:
        print(f"An error occurred: {str(e)}")
    finally:
        if cursor:
            cursor.close()
        if cnxn:
            cnxn.close()
        print(f"Connection closed!")

def read_from_postgresql(query, connection_string):
    cnxn = psycopg2.connect(connection_string)
    cursor = cnxn.cursor()
    dataframe= pd.read_sql_query(
        sql=query, con=connection_string
    )
    return dataframe



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

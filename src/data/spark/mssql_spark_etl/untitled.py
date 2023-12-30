import findspark
findspark.init()
import pyspark
from pyspark.sql import SparkSession
import os
import sys
import configparser
sys.path.append('/mnt/d/local-repo-github/enr_portfolio_modeling/')
os.chdir('D:/local-repo-github/enr_portfolio_modeling/files-storage/processed')
spark=SparkSession.builder.master('spark://pop-os.localdomain:7077').appName('blx-mdp').getOrCreate()

#Load Config
config_file=os.path.join(os.path.dirname('__file__'), 'Config/config.ini') 
config=configparser.ConfigParser(allow_no_value=True)
config.read(config_file)

JAVA_HOME=os.path.join(os.path.dirname("__file__"),config['develop']['JAVA_HOME'])
pguid=os.path.join(os.path.dirname("__file__"),config['develop']['pguid'])
pgserver=os.path.join(os.path.dirname("__file__"),config['develop']['pgserver'])
pgdwhdb=os.path.join(os.path.dirname("__file__"),config['develop']['pgdwhdb'])
pgport=os.path.join(os.path.dirname("__file__"),config['develop']['pgport'])


conf = SparkConf() \
    .setAppName("ETLPipeline") \
    .setMaster("local") \
    .set("spark.driver.extraClassPath","/mnt/d/apache-spark-dir/*")

# source connection
src_url = f"jdbc:sqlserver://{server}:1433;databaseName={src_db};user={uid};password={pwd};"
# target connection
target_url = f"jdbc:postgresql://{server}:5432/{target_db}?user={uid}&password={pwd}"

def extract():
    try:
        dfs=etl.read. \
            format("jdbc"). \
            options(driver=src_driver,user=uid, password=pwd,url=src_url,query=sql). \
            load()
        # get table names
        data_collect = dfs.collect()
        # looping thorough each row of the dataframe
        for row in data_collect:
        # while looping through each
            print(row["table_name"])
            tbl_name = row["table_name"]
            df = etl.read \
            .format("jdbc") \
            .option("driver", src_driver) \
            .option("user", uid) \
            .option("password", pwd) \
            .option("url", src_url) \
            .option("dbtable", f"dbo.{tbl_name}") \
            .load()
            #print(df.show(10))
            load(df, tbl_name)
            print("Data loaded successfully")
    except Exception as e:
        print("Data extract error: " + str(e))
        
        
def load(df, tbl):
    try:
        rows_imported = 0
        print(f'importing rows {rows_imported} to {rows_imported + df.count()}... for table {tbl}')
        df.write.mode("overwrite") \
        .format("jdbc") \
        .option("url", target_url) \
        .option("user", uid) \
        .option("password", pwd) \
        .option("driver", target_driver) \
        .option("dbtable", "src_" + tbl) \
        .save()
        print("Data imported successful")
        rows_imported += df.count()
    except Exception as e:
        print("Data load error: " + str(e))
import findspark
findspark.init()
import pyspark
from pyspark.sql import SparkSession
import os
import sys



def read_excel_file_spark(spark, path, sheet_name="Sheet1", header=True, **kwargs):
    """Function to read Excel files using PySpark.
    Parameters
    ----------
    
    Returns
    -------
    
    Example
    -------
    """
    ext = pathlib.Path(path).suffix
    if ext in ['.xlsx', '.xls', '.xlsm', '.xlsb', '.odf', '.ods', '.odt']:
        return spark.read.format("com.crealytics.spark.excel").option("header", header).option("inferSchema", True).option("sheetName", sheet_name).load(path, **kwargs)
    else:
        return spark.read.csv(path, header=header, **kwargs)
    
def read_spark_df(path:str, n:int)->'DataFrame':
    """Function to read csv file with spark
    Parameters
    ----------
    path : string
        path to csv file
    n : integer
        number of line to display
    Example
    -------
    >>>read_spark_df(path="D:/local-repo-github/enr_portfolio_modeling/files-storage/processed/production_asset.csv", n=10)
    """
    dataframe = spark\
    .read\
    .option("inferSchema", "true")\
    .option("header", "true")\
    .csv(path).show(n)
    return dataframe
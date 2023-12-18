import findspark
findspark.init()
import pyspark
from pyspark.sql import SparkSession
import os
import sys


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
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

def read_excel_file_spark(spark, path, sheet_name="Sheet1", header=True, **kwargs):
    """
    Function to read Excel files using PySpark.
    """
    ext = pathlib.Path(path).suffix
    if ext in ['.xlsx', '.xls', '.xlsm', '.xlsb', '.odf', '.ods', '.odt']:
        return spark.read.format("com.crealytics.spark.excel").option("header", header).option("inferSchema", True).option("sheetName", sheet_name).load(path, **kwargs)
    else:
        return spark.read.csv(path, header=header, **kwargs)

def extract_asset_hedge_prices_spark(spark, prod_asset_path, vol_hedge_path, template_hedge_path, template_asset_path, contract_prices_path, settl_prices_path):
    """
    Function to extract data from Excel files using PySpark.
    """
    try:
        # Read Excel files using PySpark
        schema_prod_asset = StructType([StructField('asset_id', StringType(), True),
                                       StructField('projet_id', StringType(), True),
                                       StructField('date', StringType(), True),
                                       StructField('année', StringType(), True),
                                       StructField('trim', StringType(), True),
                                       StructField('mois', StringType(), True),
                                       StructField('p50_a', StringType(), True),
                                       StructField('p90_a', StringType(), True)])
        schema_vol_hedge = StructType([StructField('hedge_id', StringType(), True),
                                      StructField('projet_id', StringType(), True),
                                      StructField('type_hedge_h', StringType(), True),
                                      StructField('date', StringType(), True),
                                      StructField('année', StringType(), True),
                                      StructField('trim', StringType(), True),
                                      StructField('mois', StringType(), True),
                                      StructField('p50_h', StringType(), True),
                                      StructField('p90_h', StringType(), True)])

        df_prod_asset = read_excel_file_spark(spark, prod_asset_path, sheet_name="Sheet1", header=True, schema=schema_prod_asset, 
                                              usecols=['asset_id', 'projet_id', 'date', 'année', 'trim', 'mois', 'p50_adj', 'p90_adj'])
        df_vol_hedge = read_excel_file_spark(spark, vol_hedge_path, sheet_name="Sheet1", header=True, schema=schema_vol_hedge, 
                                            usecols=['hedge_id', 'projet_id', 'type_hedge', 'date', 'année', 'trim', 'mois', 'p50_adj', 'p90_adj'])
        df_template_hedge = read_excel_file_spark(spark, template_hedge_path, sheet_name="Sheet1", header=True, 
                                                  usecols=['hedge_id', 'projet_id', 'date_debut', 'date_fin'])
        df_template_asset = read_excel_file_spark(spark, template_asset_path, sheet_name="Sheet1", header=True, 
                                                  usecols=['asset_id', 'projet_id', 'cod', 'date_merchant', 'date_dementelement'])
        df_contract_prices = read_excel_file_spark(spark, contract_prices_path, sheet_name="Sheet1", header=True)
        df_settlement_prices = read_excel_file_spark(spark, settl_prices_path, sheet_name="Sheet1", header=True, 
                                                     usecols=['DeliveryPeriod', 'SettlementPrice'])

        return df_prod_asset, df_vol_hedge, df_template_hedge, df_template_asset, df_contract_prices, df_settlement_prices
    except Exception as e:
        print("Data extraction error!: " + str(e))
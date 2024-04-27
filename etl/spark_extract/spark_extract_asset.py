from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
spark = SparkSession.builder.appName("ExcelReader").config("spark.jars.packages", "com.crealytics:spark-excel_2.12:0.13.6").getOrCreate()


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

def extract_asset_spark(spark, asset_vmr_path, asset_planif_path):
    """
    Function to extract data from Excel files using PySpark.
    """
    try:
        # Read Excel files using PySpark
        schema_vmr = StructType([StructField('col1', StringType(), True),  # Adjust the schema based on your actual data columns
                                 StructField('col2', StringType(), True),
                                 # Add more fields as needed
                                ])
        schema_planif = StructType([StructField('col1', StringType(), True),  # Adjust the schema based on your actual data columns
                                    StructField('col2', StringType(), True),
                                    # Add more fields as needed
                                   ])

        df_asset_vmr = read_excel_file_spark(spark, asset_vmr_path, sheet_name="vmr", header=True, schema=schema_vmr)
        df_asset_planif = read_excel_file_spark(spark, asset_planif_path, sheet_name="Planification", header=True, schema=schema_planif, 
                                                usecols=['#', 'Nom', 'Technologie', 'Puissance totale (pour les repowering)', 
                                                         'date MSI depl', "date d'entrée dans statut S", 'Taux de réussite'])

        return df_asset_vmr, df_asset_planif
    except Exception as e:
        print("Data extraction error!: " + str(e))

# Example usage:
# spark = SparkSession.builder.appName("ExcelReader").getOrCreate()
# df_asset_vmr, df_asset_planif = extract_asset_spark(spark, "path_to_asset_vmr.xlsx", "path_to_asset_planif.xlsx")

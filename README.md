# Portfolio Risk Modeling data pipeline


## 1. Architecture

#### A
![Image]( /etl_mongodb_mssql.jpeg "enr portfolio modeling")

#### B
![Image]( /etl_gcs_snowflake.jpeg "gcs ETL PIPELINE")

#### C
![Image]( /etl_google_bigquery.jpeg "gcs-Snowflake ETL PIPELINE")

## 2. Overview
Pipeline **A** Extracts data from excel files and transform and load processed data in MongoDB collections. Then data are Extracted from the staging db, modeled into facts and dimensions. And Load in SQL server data warehouse. Airflow is used for orchestration. The data dashboard is run in the cloud with dash-ploty application deployed in a Heroku VM. A SSAS multidimensionnal model is built on top of the data warehouse and SSRS dashboards and reports to display aggregated data. 

With pipeline **B**, The data are Extracted from excel source files, transform and loaded as blob file in Google Cloud Storage bucket that serves as data lake. A Snowflake Storage Integration is created and we server as staging area in snowflake datawarehouse. The data are Extracted from staging area, modeled in facts and dimensions. Then load in snowflake data warehouse. all the orchestration work is done with Airflow.

Pipeline **C** Extracts data from excel source files, transform and loaded as blob file in Google Cloud Storage bucket that serves as data lake. The transformed data are then Extracted from GCS bucket, modeled and load in Google BigQuery data warehouse. Likewise, all the orchestration work is done by Airflow. And the dashboard is run by dash-plotly and is hosted in an Heroku virtual marchine.

## 4. Data validation
Data validation tasks are handled with great_expectations.

## 4. ETL Pipeline

## 5. Staging

## 6. Data Warehouse

## 7. Dashboards
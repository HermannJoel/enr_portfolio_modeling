# Portfolio Risk Modeling data pipeline


## 1. Architecture

#### A
![Image]( /etl_mongodb_postgres.jpeg "enr portfolio modeling")

#### B
![Image]( /etl_gcs_snowflake.jpeg "gcs ETL PIPELINE")

#### C
![Image]( /etl_google_bigquery.jpeg "gcs-Snowflake ETL PIPELINE")

## 2. Technologies Used
    1. Programming Languages: - Python, -Pyspark

    2. Scripting Language: SQL

    3. Data Base: -PostgreSQL, -MongoDB, -Bigquery

    4. DWH
        - POstgreSQL
        - Snowflake

    5. Orchestrator: -Airflow
    6. Data Viz: -Dashplotly


## 3. Overview
Pipeline **A** Extracts data from excel files, transform and load processed data in MongoDB collections. Then data are Extracted from the staging db, modeled into facts and dimensions. And Load in postgres data warehouse. Airflow is used for orchestration. The data dashboard is run in the cloud with dash-ploty application deployed in a Heroku VM. 

With pipeline **B**, The data are Extracted from excel source files, transform and loaded as blob files in Google Cloud Storage bucket that serves as data lake. A Snowflake Storage Integration is created and serves as staging area in snowflake datawarehouse. The data are Extracted from staging area, modeled in facts and dimensions. Then loaded in snowflake data warehouse. all the orchestration work is done with Airflow.

Pipeline **C** Extracts data from excel source files, transform and loaded as blob file in Google Cloud Storage bucket that serves as data lake. The transformed data are then Extracted from GCS bucket, modeled and load in Google BigQuery data warehouse. Likewise, all the orchestration work is done by Airflow. And the dashboard is run by dash-plotly and is hosted in an Heroku virtual marchine.

## 4. Data validation
Data validation tasks are handled with great_expectations.

## 5. ETL Pipeline
#### 1. Mssql Pipeline 
![Image]( /pipeline_dwh.jpg "mssql pipeline")

## 6. Staging

## 7. Data Warehouse

## 8. Dashboards
![Image]( /prod.jpg "prod dashboard")

![Image]( /prod-q.jpg "quarterly prod dashboard")

![Image]( /hedge.jpg "hedge dashboard")

![Image]( /merchant.jpg "merchant dashboard")

![Image]( /mtm.jpg "mtm dashboard")

![Image]( /mtm-h.jpg "hystorical mtm dashboard")
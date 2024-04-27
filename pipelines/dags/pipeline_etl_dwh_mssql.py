from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.email_operator import EmailOperator
from airflow.sensors.external_task_sensor import ExternalTaskSensor
from airflow.utils.dates import days_ago

next_run = datetime.combine(datetime.now() + timedelta(minutes = 10),
                                      datetime.min.time())
default_args = {
    'owner': 'nherm',
    'start_date':  datetime(2024, 1, 9, 21, 00),
    'retries': 1,
    'retry_delay': timedelta(minutes = 5), 
    'email': ['hermannjoel.ngayap@gmail.com'], 
    'email_on_failure': False, 
    'email_on_retry': False,
}

python_script_path = '/mnt/d/local-repo-github/enr_portfolio_modeling/src/data/etl_xlsx_mssql/'
python_val_path = '/mnt/d/local-repo-github/enr_portfolio_modeling/test/'

dag = DAG(
    dag_id='pipeline_dwh_mssql',
    description='stagging to dwh mssql',
    schedule_interval= '0 * * * *',   # 0 * * * *(@hourly) 0 0 * * 0 (@weekly) 15 20 * * 1-7 
    default_args=default_args,
    catchup=False
    )

pipeline_stg_mssql_sensor = ExternalTaskSensor(
    task_id='pipeline_dwh_mssql_sensor',
    external_dag_id = 'pipeline_stg_mssql',
    external_task_id = 'etl_asset_stg',
    check_existence=True,
    allowed_states=['success'],
    mode = 'reschedule',
    timeout = 2500,
    poke_interval=60,
    execution_delta=timedelta(minutes=1),
    dag=dag,
)

load_asset_dwh_task = BashOperator(
    task_id='etl_asset_dwh',
    bash_command=f'python {python_script_path}'+'etl_asset_dwh_mssql.py',
    dag=dag,
    )

load_profile_dwh_task = BashOperator(
    task_id='etl_profile_dwh',
    bash_command=f'python {python_script_path}'+'etl_profile_dwh_mssql.py',
    dag=dag,
    )

load_hedge_dwh_task = BashOperator(
    task_id='etl_hedge_dwh',
    bash_command=f'python {python_script_path}'+'etl_hedge_dwh_mssql.py',
    dag=dag,
    )


check_prod_per_tech_task = BashOperator(
        task_id='check_prod_per_tech_sum',
        bash_command=f'set -e; python {python_val_path}validator.py' +
        'prod_per_tech_stg.sql prod_per_tech_dim.sql equals',
        dag=dag,
)
load_settlement_prices_dwh_task  = BashOperator(
    task_id='etl_settl_prices_dwh',
    bash_command=f'python {python_script_path}'+'etl_settlement_prices_dwh_mssql.py',
    dag=dag,
    )

load_contract_prices_dwh_task  = BashOperator(
    task_id='etl_contract_prices_dwh',
    bash_command=f'python {python_script_path}'+'etl_contract_prices_dwh_mssql.py',
    dag=dag,
    )

load_prod_asset_dwh_task  = BashOperator(
    task_id='etl_prod_asset_dwh',
    bash_command=f'python {python_script_path}'+'etl_prod_dwh_mssql.py',
    dag=dag,
    )
load_vol_hedge_dwh_task  = BashOperator(
    task_id='etl_vol_hedge_dwh',
    bash_command=f'python {python_script_path}'+'etl_vol_hedge_dwh_mssql.py',
    dag=dag,
    )

pipeline_stg_mssql_sensor >> load_asset_dwh_task >> load_profile_dwh_task >> load_hedge_dwh_task >> check_prod_per_tech_task >>[load_settlement_prices_dwh_task, load_contract_prices_dwh_task, load_prod_asset_dwh_task, load_vol_hedge_dwh_task] 
if __name__ == "__main__":
    dag.cli()
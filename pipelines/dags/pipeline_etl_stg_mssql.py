from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.email_operator import EmailOperator
from airflow.sensors.external_task_sensor import ExternalTaskSensor
from airflow.utils.dates import days_ago

next_run = datetime.combine(datetime.now() + timedelta(minutes = 5),
                                      datetime.min.time())
default_args = {
    'owner': 'nherm',
    'start_date': datetime(2024, 1, 9, 21, 00),
    'max_active_runs': 1,
    'retries': 1,
    'retry_delay': timedelta(minutes = 5), 
    'email': ['hermannjoel.ngayap@gmail.com'], 
    'email_on_failure': False, 
    'email_on_retry': False,
}

python_script_path = '/mnt/d/local-repo-github/enr_portfolio_modeling/src/data/etl_xlsx_mssql/'

dag = DAG(
    dag_id='pipeline_stg_mssql',
    description='xlsx to stagging mssql',
    schedule_interval= '0 * * * *',   # 0 * * * *(@hourly) 0 0 * * 0 (@weekly) 15 20 * * 1-7
    default_args=default_args,
    catchup=False
    )

etl_xlsx_csv_sensor = ExternalTaskSensor(
    task_id='pipeline_xlsx_csv_sensor',
    external_dag_id = 'pipeline_xlsx_csv',
    external_task_id = 'etl_asset',
    check_existence=True,
    allowed_states=['success'],
    mode = 'reschedule',
    timeout = 2500,
    poke_interval=60,
    execution_delta=timedelta(minutes=1),
    dag=dag,
)

load_asset_stg_task = BashOperator(
    task_id='etl_asset_stg',
    bash_command=f'python {python_script_path}'+'etl_asset_stg_mssql.py',
    dag=dag,
    )

load_profile_stg_task = BashOperator(
    task_id='etl_profile_stg',
    bash_command=f'python {python_script_path}'+'etl_profile_stg_mssql.py',
    dag=dag,
    )

load_hedge_stg_task = BashOperator(
    task_id='etl_hedge_stg',
    bash_command=f'python {python_script_path}'+'etl_hedge_stg_mssql.py',
    dag=dag,
    )

load_prices_stg_task  = BashOperator(
    task_id='etl_prices_stg',
    bash_command=f'python {python_script_path}'+'etl_prices_stg_mssql.py',
    dag=dag,
    )

load_settlement_prices_stg_task  = BashOperator(
    task_id='etl_settl_prices_stg',
    bash_command=f'python {python_script_path}'+'etl_settlement_prices_stg_mssql.py',
    dag=dag,
    )

load_contract_prices_stg_task  = BashOperator(
    task_id='etl_contract_prices_stg',
    bash_command=f'python {python_script_path}'+'etl_contract_prices_stg_mssql.py',
    dag=dag,
    )

load_prod_asset_stg_task  = BashOperator(
    task_id='etl_prod_asset_stg',
    bash_command=f'python {python_script_path}'+'etl_prod_stg_mssql.py',
    dag=dag,
    )
load_vol_hedge_stg_task  = BashOperator(
    task_id='etl_vol_hedge_stg',
    bash_command=f'python {python_script_path}'+'etl_vol_hedge_stg_mssql.py',
    dag=dag,
    )
etl_xlsx_csv_sensor >> load_asset_stg_task >> load_profile_stg_task >> load_hedge_stg_task >> load_prices_stg_task >>[load_settlement_prices_stg_task, load_contract_prices_stg_task, load_prod_asset_stg_task, load_vol_hedge_stg_task] 
if __name__ == "__main__":
    dag.cli()
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.email_operator import EmailOperator
from airflow.sensors.external_task_sensor import ExternalTaskSensor
from airflow.utils.dates import days_ago

next_run = datetime.combine(datetime.now() + timedelta(hours = 1),
                                      datetime.min.time())
default_args = {
    'owner': 'nherm',
    'start_date': next_run,
    'retries': 1,
    'retry_delay': timedelta(hours = 1), 
    'email': ['hermannjoel.ngayap@gmail.com'], 
    'email_on_failure': True, 
    'email_on_retry': False,
}

python_script_path = '/mnt/d/local-repo-github/enr_portfolio_modeling/src/data/etl_xlsx_mssql/'
python_val_path = '/mnt/d/local-repo-github/enr_portfolio_modeling/test/'

dag = DAG(
    'pipeline_mssql',
    description='xlsx to mssql',
    schedule_interval= '0 21 * * 1-7',   # 0 * * * *(@hourly) 0 0 * * 0 (@weekly)
    default_args=default_args
    )

etl_as_xlsx_sensor = ExternalTaskSensor(
    task_id='pipeline_xlsx_csv_sensor',
    external_dag_id = 'pipeline_xlsx_csv',
    external_task_id = None,
    dag=dag,
    mode = 'reschedule',
    timeout = 2500,
    execution_delta=timedelta(minutes=30)
)

asset_task = BashOperator(
    task_id='etl_asset',
    bash_command=f'python {python_script_path}'+'etl_asset_mssql.py',
    dag=dag,
    )

profile_task = BashOperator(
    task_id='etl_profile',
    bash_command=f'python {python_script_path}'+'etl_profile_mssql.py',
    dag=dag,
    )

hedge_task = BashOperator(
    task_id='etl_hedge',
    bash_command=f'python {python_script_path}'+'etl_hedge_mssql.py',
    dag=dag,
    )

hedge_scd2_task = PostgresOperator(
    task_id='hedge_scd2',
    postgres_conn_id='warehouse_con',
    sql=f'python {python_script_path}'+'hedge_scd2.sql',
    dag=dag,
    )

prices_task  = BashOperator(
    task_id='etl_prices',
    bash_command=f'python {python_script_path}'+'etl_prices_mssql.py',
    dag=dag,
    )

check_prod_per_tech_task = BashOperator(
        task_id='check_prod_per_tech_sum',
        bash_command=f'set -e; python {python_val_path}validator.py' +
        'prod_per_tech_stg.sql prod_per_tech_dim.sql equals',
        dag=dag,
)
settlement_prices_task  = BashOperator(
    task_id='etl_settl_prices',
    bash_command=f'python {python_script_path}'+'etl_settlement_prices_mssql.py',
    dag=dag,
    )

contract_prices_task  = BashOperator(
    task_id='etl_contract_prices',
    bash_command=f'python {python_script_path}'+'etl_contract_prices_mssql.py',
    dag=dag,
    )

prod_asset_task  = BashOperator(
    task_id='etl_prod_asset',
    bash_command=f'python {python_script_path}'+'etl_prod_xlsx.py',
    dag=dag,
    )
vol_hedge_task  = BashOperator(
    task_id='etl_vol_hedge',
    bash_command=f'python {python_script_path}'+'etl_vol_hedge_xlsx.py',
    dag=dag,
    )
# email_task = EmailOperator(
#      task_id='send_email_on_failure',
#      to="hermannjoel.ngayap@gmail.com",
#      subject="pipeline_xls_xlsxcsv failed",
#      html_content='This batch did not run successfully. Check you logs',
#       )

etl_as_xlsx_sensor >> asset_task >> profile_task >> hedge_task >> prices_task >> check_prod_per_tech_task >>[settlement_prices_task, contract_prices_task, prod_asset_task, vol_hedge_task] 
#>> email_task
# etl_as_xlsx_sensor >> asset_task
# etl_as_xlsx_sensor >> profile_task >> hedge_task >> prices_task >> settlement_prices_task
# asset_task >> check_prod_per_tech_task >> settlement_prices_task
# contract_prices_task >> prod_asset_task >> vol_hedge_task >> email_task
if __name__ == "__main__":
    dag.cli()
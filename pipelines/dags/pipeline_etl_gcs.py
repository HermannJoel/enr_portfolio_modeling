from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.utils.dates import days_ago


next_run = datetime.combine(datetime.now() + timedelta(minutes = 15),
                                      datetime.min.time())
default_args = {
    'owner': 'nherm',
    'start_date': next_run,
    'retries': 1,
    'retry_delay': timedelta(minutes = 5), 
    'email': ['hermannjoel.ngayap@yahoo.fr'], 
    'email_on_failure': False, 
    'email_on_retry': False,
}

python_script_path = '/mnt/d/local-repo-github/enr_portfolio_modeling/src/data/etl_gcs_bigquery/'

dag = DAG(
    'pipeline_to_gcs',
    description='xlsx to gcs',
    schedule_interval= '0 20 * * 1-7',   # 0 * * * *(@hourly) 0 0 * * 0 (@weekly)
    default_args=default_args
    )
asset_task = BashOperator(
    task_id='etl_asset',
    bash_command='python {python_script_path}'+'etl_asset_gcp.py',
    dag=dag,
    )

profile_task = BashOperator(
    task_id='etl_profile',
    bash_command='python {python_script_path}'+'etl_profile_gcp.py',
    dag=dag,
    )

hedge_task = BashOperator(
    task_id='etl_hedge',
    bash_command='python {python_script_path}'+'etl_hedge_gcp.py',
    dag=dag,
    )

prices_task  = BashOperator(
    task_id='etl_prices',
    bash_command='python {python_script_path}'+'etl_prices_gcp.py',
    dag=dag,
    )

settlement_prices_task  = BashOperator(
    task_id='etl_settl_prices',
    bash_command='python {python_script_path}'+'etl_settlement_prices_gcp.py',
    dag=dag,
    )

contract_prices_task  = BashOperator(
    task_id='etl_contract_prices',
    bash_command='python {python_script_path}'+'etl_contract_prices_gcp.py',
    dag=dag,
    )

prod_asset_task  = BashOperator(
    task_id='etl_prod_asset',
    bash_command='{python_script_path}'+'etl_prod_gcp.py',
    dag=dag,
    )
vol_hedge_task  = BashOperator(
    task_id='etl_vol_hedge',
    bash_command='python {python_script_path}'+'etl_vol_hedge_gcp.py',
    dag=dag,
    )

asset_task >> profile_task >> hedge_task >> prices_task >> settlement_prices_task >> contract_prices_task >> prod_asset_task >> vol_hedge_task

if __name__ == "__main__":
    dag.cli()
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

python_script_path = '/mnt/d/local-repo-github/enr_portfolio_modeling/src/data/etl_gcs_snowflake/'

with DAG(
     'pipeline_snowflake',
     description='xlsx to snowflake',
     schedule_interval='0 20 * * 1-7',
     default_args=default_args
 ) as dag:
    
    asset_task = BashOperator(
        task_id='etl_asset',
        bash_command='python {python_script_path}'+'etl_asset_sf.py',
    )

    profile_task = BashOperator(
        task_id='etl_profile',
        bash_command='python {python_script_path}'+'etl_profile_sf.py',
        )

    hedge_task = BashOperator(
        task_id='etl_hedge',
        bash_command='python {python_script_path}'+'etl_hedge_sf.py',
        )

    prices_task  = BashOperator(
        task_id='etl_prices',
        bash_command='python {python_script_path}'+'etl_prices_sf.py',
        )

    settlement_prices_task  = BashOperator(
        task_id='etl_settl_prices',
        bash_command='python {python_script_path}'+'etl_settlement_prices_sf.py',
        )

    contract_prices_task  = BashOperator(
        task_id='etl_contract_prices',
        bash_command='python {python_script_path}'+'etl_contract_prices_sf.py',
        )

    prod_asset_task  = BashOperator(
        task_id='etl_prod_asset',
        bash_command='python {python_script_path}'+'etl_prod_sf.py'
        )
    vol_hedge_task  = BashOperator(
        task_id='etl_vol_hedge',
        bash_command='python {python_script_path}'+'etl_vol_hedge_sf.py'
        )

#asset_task >> profile_task >> hedge_task >> prices_task >> settlement_prices_task >> contract_prices_task >> prod_asset_task >> vol_hedge_task
asset_task >> profile_task
profile_task >> hedge_task >> prices_task >> settlement_prices_task
asset_task >> settlement_prices_task
settlement_prices_task >> contract_prices_task >> prod_asset_task >> vol_hedge_task

if __name__ == "__main__":
    dag.cli()
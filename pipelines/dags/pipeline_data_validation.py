
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

dag = DAG(
    'pipeline_to_validate_data',
    description='xlsx to xlsx_csv',
    schedule_interval= '0 * * * *',   # 0 * * * *(@hourly) 0 0 * * 0 (@weekly)
    default_args=default_args
    )
validate_asset_task = BashOperator(
    task_id='validate_etl_asset',
    bash_command='python /mnt/d/local-repo-github/enr_portfolio_modeling/src/data/etl_xlsx-xlsxcsv/etl_asset_xlsx.py',
    dag=dag,
    )

validate_profile_task = BashOperator(
    task_id='validate_etl_profile',
    bash_command='python /mnt/d/local-repo-github/enr_portfolio_modeling/src/data/etl_xlsx-xlsxcsv/etl_profile_xlsx.py',
    dag=dag,
    )

validate_hedge_task = BashOperator(
    task_id='validate_etl_hedge',
    bash_command='python /mnt/d/local-repo-github/enr_portfolio_modeling/src/data/etl_xlsx-xlsxcsv/etl_hedge_xlsx.py',
    dag=dag,
    )

validate_prices_task  = BashOperator(
    task_id='validate_etl_prices',
    bash_command='python /mnt/d/local-repo-github/enr_portfolio_modeling/src/data/etl_xlsx-xlsxcsv/etl_prices_xlsx.py',
    dag=dag,
    )

validate_settl_prices_task  = BashOperator(
    task_id='validate_etl_settl_prices',
    bash_command='python /mnt/d/local-repo-github/enr_portfolio_modeling/src/data/etl_xlsx-xlsxcsv/etl_settlement_prices_xlsx.py',
    dag=dag,
    )

validate_contract_prices_task  = BashOperator(
    task_id='validate_etl_contract_prices',
    bash_command='python /mnt/d/local-repo-github/enr_portfolio_modeling/src/data/etl_xlsx-xlsxcsv/etl_contract_prices_xlsx.py',
    dag=dag,
    )

validate_prod_asset_task  = BashOperator(
    task_id='validate_etl_prod_asset',
    bash_command='python /mnt/d/local-repo-github/enr_portfolio_modeling/src/data/etl_xlsx-xlsxcsv/etl_prod_xlsx.py',
    dag=dag,
    )
validate_vol_hedge_task  = BashOperator(
    task_id='validate_etl_vol_hedge',
    bash_command='python /mnt/d/local-repo-github/enr_portfolio_modeling/src/data/etl_xlsx-xlsxcsv/etl_vol_hedge_xlsx.py',
    dag=dag,
    )

validate_asset_task >> validate_profile_task >> validate_hedge_task >> validate_prices_task >> [validate_settl_prices_task, validate_contract_prices_task, validate_prod_asset_task, validate_vol_hedge_task]
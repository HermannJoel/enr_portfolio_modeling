#Scheduler: uses the executor
#Executors: are what Airflow uses to run tasks that the Scheduler determines are ready to run.
#Operators: are what actually execute scripts, commands, and other operations

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.utils.dates import days_ago

next_run = datetime.combine(datetime.now() + timedelta(days = 1),
                                      datetime.min.time())
default_args = {
    'owner': 'nherm',
    'start_date': next_run,
    'retries': 1,
    'retry_delay': timedelta(hours = 1), 
    'email': ['hermannjoel.ngayap@yahoo.fr'], 
    'email_on_failure': True, 
    'email_on_retry': False,
}

dag = DAG(
    'pipeline_xls_xlsxcsv',
    description='xlsxcsv to templates',
    schedule_interval='0 20 * * 1-5',
    default_args=default_args
    )

python_script_path = '/mnt/d/local-repo-github/enr_portfolio_modeling/src/data/etl_xlsx_xlsxcsv/'

create_asset_task = BashOperator(
     task_id='etl_asset',
     bash_command=f'python {python_script_path}'+'etl_asset_xlsx.py',
     dag=dag,
     )

create_profile_task = BashOperator(
    task_id='etl_profile',
    bash_command=f'python {python_script_path}'+'etl_profile_xlsx.py',
    dag=dag,
    )
create_hedge_task = BashOperator(
    task_id='etl_hedge',
    bash_command=f'python {python_script_path}'+'etl_hedge_xlsx.py',
    dag=dag,
    )
create_prices_task  = BashOperator(
    task_id='etl_prices',
    bash_command=f'python {python_script_path}'+'etl_prices_xlsx.py',
    dag=dag,
    )
# compute_settl_prices_task  = BashOperator(
#     task_id='etl_settl_prices',
#     bash_command=f'python {python_script_path}'+'etl_settlement_prices_xlsx.py',
#     dag=dag,
#     )

compute_contract_prices_task  = BashOperator(
    task_id='etl_contract_prices',
    bash_command=f'python {python_script_path}'+'etl_contract_prices_xlsx.py',
    dag=dag,
    )

compute_prod_asset_task  = BashOperator(
    task_id='etl_prod_asset',
    bash_command=f'python {python_script_path}'+'etl_prod_xlsx.py',
    dag=dag,
    )
compute_vol_hedge_task  = BashOperator(
    task_id='etl_vol_hedge',
    bash_command=f'python {python_script_path}'+'etl_vol_hedge_xlsx.py',
    dag=dag,
    )

#create_asset_task >> create_profile_task >> create_hedge_task >> create_prices_task >> [compute_settl_prices_task, compute_contract_prices_task, #compute_prod_asset_task, compute_vol_hedge_task]

create_asset_task >> create_profile_task >> create_hedge_task >> create_prices_task >> [compute_contract_prices_task, compute_prod_asset_task, compute_vol_hedge_task]

if __name__ == "__main__":
    dag.cli()

"""
#create_tp_asset_task >> create_tp_productibles_task
#create_tp_productibles_task >> create_tp_hedge_task 
#create_tp_hedge_task  >> compute_p50_p90_asset_task
#compute_p50_p90_asset_task >> compute_volume_hedge_task
#create_tp_asset_task.set_downstream(create_tp_productibles_task) #task 2&3 will run in parallel after task 1
#create_tp_asset_task.set_downstream(create_tp_hedge_task)
#create_tp_asset_task[create_tp_productibles_task, create_tp_hedge_task]
"""




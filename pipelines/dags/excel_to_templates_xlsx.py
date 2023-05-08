#Scheduler: uses the executor
#Executors: are what Airflow uses to run tasks that the Scheduler determines are ready to run.
#Operators: are what actually execute scripts, commands, and other operations
#import os
#os.chdir('D:/local-repo-github/enr_portfolio_modeling/')

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
#from airflow.operators.postgres_operator import PostgresOperator
from airflow.utils.dates import days_ago
#from src.data import etl_hedge


next_run = datetime.combine(datetime.now() + timedelta(hours = 12),
                                      datetime.min.time())
default_args = {
    'owner': 'nherm',
    'start_date': next_run,
    'retries': 1,
    'retry_delay': timedelta(minutes = 10), 
    'email': ['hermannjoel.ngayap@yahoo.fr'], 
    'email_on_failure': False, 
    'email_on_retry': False,
}

dag = DAG(
    'elt_pipeline_enr_portfolio_modeling',
    description='xlsx to dbs',
    schedule_interval=timedelta(days=1),
    default_args=default_args
    )
create_template_asset_task = BashOperator(
    task_id='etl_asset',
    bash_command='python /mnt/d/local-repo-github/enr_portfolio_modeling/src/data/etl_asset.py',
    dag=dag,
    )

create_template_profile_task = BashOperator(
    task_id='etl_profile',
    bash_command='python /mnt/d/local-repo-github/enr_portfolio_modeling/src/data/etl_profile.py',
    dag=dag,
    )
create_template_hedge_task = BashOperator(
    task_id='etl_hedge',
    bash_command='python /mnt/d/local-repo-github/enr_portfolio_modeling/src/data/etl_hedge.py',
    dag=dag,
    )
compute_template_prices_task  = BashOperator(
    task_id='etl_prices',
    bash_command='python /mnt/d/local-repo-github/enr_portfolio_modeling/src/data/etl_prices.py',
    dag=dag,
    )

create_contract_prices_task  = BashOperator(
    task_id='etl_contract_prices',
    bash_command='python /mnt/d/local-repo-github/enr_portfolio_modeling/src/data/etl_contract_prices.py',
    dag=dag,
    )

compute_prod_asset_task  = BashOperator(
    task_id='etl_prod_asset',
    bash_command='python /mnt/d/local-repo-github/enr_portfolio_modeling/src/data/etl_prod.py',
    dag=dag,
    )
compute_vol_hedge_task  = BashOperator(
    task_id='etl_vol_hedge',
    bash_command='python /mnt/d/local-repo-github/enr_portfolio_modeling/src/data/etl_vol_hedge.py',
    dag=dag,
    )

create_template_asset_task >> create_template_profile_task
create_template_profile_task >> create_template_hedge_task  
create_template_hedge_task >> compute_template_prices_task
compute_template_prices_task >> create_contract_prices_task
create_contract_prices_task >> compute_prod_asset_task
compute_prod_asset_task >> compute_vol_hedge_task

"""
#create_tp_asset_task >> create_tp_productibles_task
#create_tp_productibles_task >> create_tp_hedge_task 
#create_tp_hedge_task  >> compute_p50_p90_asset_task
#compute_p50_p90_asset_task >> compute_volume_hedge_task
#create_tp_asset_task.set_downstream(create_tp_productibles_task) #task 2&3 will run in parallel after task 1
#create_tp_asset_task.set_downstream(create_tp_hedge_task)
#create_tp_asset_task[create_tp_productibles_task, create_tp_hedge_task]
"""
"""
import datetime
from datetime import timedelta
from airflow.operators.bash_operator import BashOperator
from airflow.utils.dates import days_ago
from airflow import DAG
from airflow.operators.python_operator import PythonOperator


default_args = {
'owner': 'nherm',
'start_date': dt.datetime(2023, 3, 17),
'retries': 1,
'retry_delay': dt.timedelta(minutes=5),
}

with DAG('create_templates',
         default_args=default_args,
         schedule_interval='@weekly',
         description='pipeline to create templates',
         start_date=datetime(2023, 03, 24)
) as dag:
    create_tp_asset_task = PythonOperator(
    task_id='template_asset',
    python='blx_mdp_data-eng/etls/',
    python_callable= "main_etl_template_asset.py")
    
    
    create_tp_productibles_task = PythonOperator(
        task_id='template_prod',
        python='blx_mdp_data-eng/etls/',
        python_callable= 'main_etl_template_productibles.py')
    
    create_tp_hedge_task = PythonOperator(
        task_id='template_hedge',
        python='blx_mdp_data-eng/etls/'
        python_callable='main_etl_template_hedge.py')
    
    compute_p50_p90_asset_task = PythonOperator(
        task_id='p50_p90_asset',
        python='blx_mdp_data-eng/etls/',
        python_callable='main_etl_p50_p90_asset.py')
    
    compute_volume_hedge_task = PythonOperator(
        task_id='volume_hedge',
        python='blx_mdp_data-eng/etls/',
        python_callable='main_etl_volume_hedge.py')
    
create_tp_asset_task >>  create_tp_productibles_task
create_tp_hedge_task >> compute_p50_p90_asset_task
compute_volume_hedge_task
"""




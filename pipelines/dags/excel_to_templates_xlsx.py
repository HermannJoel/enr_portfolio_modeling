#Scheduler: uses the executor
#Executors: are what Airflow uses to run tasks that the Scheduler determines are ready to run.
#Operators: are what actually execute scripts, commands, and other operations

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.utils.dates import days_ago


x_days_ago = datetime.combine(datetime.today() - timedelta(1),
                                      datetime.min.time())
default_args = {
'owner': 'nherm',
'start_date': x_days_ago,
'retries': 1,
'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'elt_pipeline_to_generate_templates',
    description='xlsx to templates',
    schedule_interval=timedelta(days=1),
    start_date = days_ago(1),
    default_args=default_args
    )
create_template_asset_task = BashOperator(
    task_id='etl_template_asset',
    bash_command='python /blx_mdp_data-eng/etls/main_etl_template_asset.py',
    dag=dag,
    )

create_template_productibles_task = BashOperator(
    task_id='etl_template_prod',
    bash_command='python /blx_mdp_data-eng/etls/main_etl_template_productibles.py',
    dag=dag,
    )
create_template_hedge_task = BashOperator(
    task_id='etl_template_hedge',
    bash_command='python /blx_mdp_data-eng/etls/main_etl_template_hedge.py',
    dag=dag,
    )

create_template_price_task  = BashOperator(
    task_id='etl_template_prices',
    bash_command='python /blx_mdp_data-eng/etls/main_etl_template_prices.py',
    dag=dag,
    )

compute_p50_p90_asset_task  = BashOperator(
    task_id='etl_p50_p90_asset',
    bash_command='python /blx_mdp_data-eng/etls/main_etl_p50_p90_asset.py',
    dag=dag,
    )
"""
compute_volume_hedge_task  = BashOperator(
    task_id='etl_volume_hedge',
    bash_command='python /blx_mdp_data-eng/etls/main_etl_volume_hedge.py',
    dag=dag,
    )
"""

create_template_asset_task >> create_template_productibles_task
create_template_productibles_task >> create_template_hedge_task  
create_template_hedge_task[create_template_prices_task, compute_p50_p90_asset_task]
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




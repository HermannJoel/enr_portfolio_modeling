from datetime import datetime, timedelta
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.bash_operator import BashOperator
from airflow.operators.email_operator import EmailOperator
from airflow.models import Variable
from airflow.utils.task_group import TaskGroup
from airflow.sensors.external_task_sensor import ExternalTaskSensor
import pendulum


local_tz = pendulum.timezone("Europe/Paris")
next_run = datetime.combine(datetime.now() + timedelta(minutes = 15),
                                       datetime.min.time())
                                       
                                       
default_args = {
    'owner': 'nherm',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 9, 21, 00),
    'max_active_runs': 1,
    'retries': 1,
    'retry_delay': timedelta(minutes = 5), 
    'email': ['hermannjoel.ngayap@gmail.com'], 
    'email_on_failure': False, 
    'email_on_retry': False,
}


python_script_path = '/mnt/d/local-repo-github/enr_portfolio_modeling/src/data/etl_xlsx_xlsxcsv/'
python_val_path = '/mnt/d/local-repo-github/enr_portfolio_modeling/test/'

with DAG(
    'enr_portfolio_modeling_pipeline',
    description='load enr data into datawarehouse',
    schedule_interval='0 20 * * 1-7',
    catchup=True,
    default_args=default_args) as dag:
  
        with TaskGroup(group_id='create_templates') as create_templates:
        
            create_asset_template_task = BashOperator( 
                task_id='asset_template',
                bash_command=f'python {python_script_path}'+'etl_asset_xlsx.py',  
                )
            create_profile_template_task = BashOperator(
                task_id='template_profile',
                bash_command=f'python {python_script_path}'+'etl_profile_xlsx.py',
                )
                
            create_hedge_template_task = BashOperator(
                task_id='template_hedge',
                bash_command=f'python {python_script_path}'+'etl_hedge_xlsx.py',
                )
                
            create_prices_template_task  = BashOperator(
                task_id='prices_template',
                bash_command=f'python {python_script_path}'+'etl_prices_xlsx.py',
                )
            create_asset_template_task >> create_profile_template_task >> create_hedge_template_task >> create_prices_template_task
         
         
        with TaskGroup(group_id='compute_prices_prod') as compute_prices_prod: 
        
            compute_settl_prices_task  = BashOperator(
                task_id='etl_settl_prices',
                bash_command=f'python {python_script_path}'+'etl_settlement_prices_xlsx.py',
                )

            compute_contract_prices_task  = BashOperator(
                task_id='etl_contract_prices',
                bash_command=f'python {python_script_path}'+'etl_contract_prices_xlsx.py',
                )
            compute_prod_asset_task  = BashOperator(
                task_id='etl_prod_asset',
                bash_command=f'python {python_script_path}'+'etl_prod_xlsx.py',
                )
            compute_vol_hedge_task  = BashOperator( 
                task_id='etl_vol_hedge',
                bash_command=f'python {python_script_path}'+'etl_vol_hedge_xlsx.py',
                )
            
            compute_settl_prices_task >> compute_prod_asset_task >> compute_vol_hedge_task
            compute_contract_prices_task >> compute_prod_asset_task >> compute_vol_hedge_task
            
        with TaskGroup(group_id='load_staging') as load_staging: 
        
            templates_sensor = ExternalTaskSensor( 
                task_id='templates_sensor',
                external_dag_id = 'create_templates',
                external_task_group_id ='create_templates',
                allowed_states=['success'],
                poke_interval = 60*30,
                timeout=60*60,
                execution_delta=timedelta(minutes=15) 
                )
            load_asset_stg_task = BashOperator(
                task_id='etl_asset_stg',
                bash_command=f'python {python_script_path}'+'etl_asset_stg_mssql.py',
                )

            load_profile_stg_task = BashOperator(
                task_id='etl_profile_stg',
                bash_command=f'python {python_script_path}'+'etl_profile_stg_mssql.py',
                )

            load_hedge_stg_task = BashOperator(
                task_id='etl_hedge_stg',
                bash_command=f'python {python_script_path}'+'etl_hedge_stg_mssql.py',
                )

            load_prices_stg_task  = BashOperator(
                task_id='etl_prices_stg',
                bash_command=f'python {python_script_path}'+'etl_prices_stg_mssql.py',
                )

            load_settlement_prices_stg_task  = BashOperator(
                task_id='etl_settl_prices_stg',
                bash_command=f'python {python_script_path}'+'etl_settlement_prices_stg_mssql.py',
                )

            load_contract_prices_stg_task  = BashOperator(
                task_id='etl_contract_prices_stg',
                bash_command=f'python {python_script_path}'+'etl_contract_prices_stg_mssql.py',
                )

            load_prod_asset_stg_task  = BashOperator(
                task_id='etl_prod_asset_stg',
                bash_command=f'python {python_script_path}'+'etl_prod_stg_mssql.py',
                )
            load_vol_hedge_stg_task  = BashOperator(
                task_id='etl_vol_hedge_stg',
                bash_command=f'python {python_script_path}'+'etl_vol_hedge_stg_mssql.py',
                )

            templates_sensor >> load_asset_stg_task >> load_prod_asset_stg_task >> load_vol_hedge_stg_task
            templates_sensor >> load_profile_stg_task >> load_prod_asset_stg_task >> load_vol_hedge_stg_task
            templates_sensor >> load_hedge_stg_task >> load_prod_asset_stg_task >> load_vol_hedge_stg_task
            templates_sensor >> load_prices_stg_task >> load_prod_asset_stg_task >> load_vol_hedge_stg_task
            templates_sensor >> load_prices_stg_task >> load_contract_prices_stg_task >> load_prod_asset_stg_task >> load_vol_hedge_stg_task
            templates_sensor >> load_settlement_prices_stg_task >> load_prod_asset_stg_task >> load_vol_hedge_stg_task
            
        with TaskGroup(group_id='load_dwh') as load_dwh:
        
            staging_sensor = ExternalTaskSensor(
                task_id='pipeline_load_stg_sensor',
                external_dag_id = 'load_staging',
                external_task_group_id = 'load_staging',
                allowed_states=['success'],
                poke_interval = 60*30,
                timeout=60*60,
                execution_delta=timedelta(minutes=15),
                dag=dag,
                )

            load_asset_dwh_task = BashOperator(
                task_id='etl_asset_dwh',
                bash_command=f'python {python_script_path}'+'etl_asset_dwh_mssql.py', 
                )

            load_profile_dwh_task = BashOperator(
                task_id='etl_profile_dwh',
                bash_command=f'python {python_script_path}'+'etl_profile_dwh_mssql.py',
                )

            load_hedge_dwh_task = BashOperator(
                task_id='etl_hedge_dwh',
                bash_command=f'python {python_script_path}'+'etl_hedge_dwh_mssql.py',
                )

            check_prod_per_tech_task = BashOperator(
                task_id='check_prod_per_tech_sum',
                bash_command=f'set -e; python {python_val_path}validator.py' +
                'prod_per_tech_stg.sql prod_per_tech_dim.sql equals',
                )
                
            load_settlement_prices_dwh_task  = BashOperator(
                task_id='etl_settl_prices_dwh',
                bash_command=f'python {python_script_path}'+'etl_settlement_prices_dwh_mssql.py',
                )

            load_contract_prices_dwh_task  = BashOperator(
                task_id='etl_contract_prices_dwh',
                bash_command=f'python {python_script_path}'+'etl_contract_prices_dwh_mssql.py',
                )

            load_prod_asset_dwh_task  = BashOperator(
                task_id='etl_prod_asset_dwh',
                bash_command=f'python {python_script_path}'+'etl_prod_dwh_mssql.py',
                )
            load_vol_hedge_dwh_task  = BashOperator(
                task_id='etl_vol_hedge_dwh',
                bash_command=f'python {python_script_path}'+'etl_vol_hedge_dwh_mssql.py',
                )
                
            staging_sensor >> load_asset_dwh_task >> check_prod_per_tech_task >> load_prod_asset_dwh_task   
            staging_sensor >> load_profile_dwh_task >> check_prod_per_tech_task >> load_contract_prices_dwh_task >> load_settlement_prices_dwh_task
            staging_sensor >> load_hedge_dwh_task >> check_prod_per_tech_task >> load_vol_hedge_dwh_task 
        
create_templates >> load_staging >> load_dwh
compute_prices_prod >> load_staging >> load_dwh
            
if __name__ == "__main__":
    dag.cli()           

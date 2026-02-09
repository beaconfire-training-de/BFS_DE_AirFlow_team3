from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
import os
from datetime import datetime

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

def load_sql(*paths):
    file_path = os.path.join(BASE_DIR, *paths)
    with open(file_path, 'r') as f:
        return f.read()
    
    
with DAG(
    dag_id = 'stock_hist_3',
    start_date= datetime(2026, 1, 1),
    schedule_interval=None,
    catchup= False,
    tags = ['team3', 'init']
) as dag:
    create_staging_tables = SnowflakeOperator(
        task_id = 'create_staging_tables',
        sql = load_sql('create-staging-tables.sql'),
        snowflake_conn_id= 'jan_airflow_snowflake'
        )

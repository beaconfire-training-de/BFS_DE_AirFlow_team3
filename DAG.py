from airflow import DAG
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from datetime import datetime, timedelta

with DAG(
    dag_id="stock_dwh_etl_team3",
    start_date=datetime(2024, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    default_args=default_args,
    tags=['team3', 'stock', 'dwh', 'scd'],
) as dag:

    create_tables = SnowflakeOperator(
        task_id="create_tables",
        snowflake_conn_id="jan_airflow_snowflake",
        sql="sql/create_tables.sql",
    )

    load_dim_symbol = SnowflakeOperator(
        task_id="load_dim_symbol",
        snowflake_conn_id="jan_airflow_snowflake",
        sql="sql/load_dim_symbol.sql",
    )

    load_dim_company = SnowflakeOperator(
        task_id="load_dim_company",
        snowflake_conn_id="jan_airflow_snowflake",
        sql="sql/load_dim_company.sql",
    )

    load_dim_date = SnowflakeOperator(
        task_id="load_dim_date",
        snowflake_conn_id="jan_airflow_snowflake",
        sql="sql/load_dim_date.sql",
    )

    load_fact_stock_history = SnowflakeOperator(
        task_id="load_fact_stock_history",
        snowflake_conn_id="jan_airflow_snowflake",
        sql="sql/load_fact_stock_history.sql",
    )

    # Task dependencies
    create_tables >> load_dim_symbol >> load_dim_company >> load_dim_date >> load_fact_stock_history

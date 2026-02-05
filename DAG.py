from airflow import DAG
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from datetime import datetime

with DAG(
    dag_id="stock_etl_daily",
    start_date=datetime(2024, 1, 1),
    schedule_interval="0 2 * * *",  # Run daily at 2:00 AM
    catchup=False,
    default_args={"owner": "team3"},
    tags=["stock", "ETL", "daily"]
) as dag:

    create_tables = SnowflakeOperator(
        task_id="create_tables",
        snowflake_conn_id="jan_airflow_snowflake",
        sql="sql/create_tables.sql"
    )

    load_dim_symbol = SnowflakeOperator(
        task_id="load_dim_symbol",
        snowflake_conn_id="jan_airflow_snowflake",
        sql="sql/load_dim_symbol.sql"
    )

    load_dim_company = SnowflakeOperator(
        task_id="load_dim_company",
        snowflake_conn_id="jan_airflow_snowflake",
        sql="sql/load_dim_company.sql"
    )

    load_dim_date = SnowflakeOperator(
        task_id="load_dim_date",
        snowflake_conn_id="jan_airflow_snowflake",
        sql="sql/load_dim_date.sql"
    )

    load_fact_stock_history = SnowflakeOperator(
        task_id="load_fact_stock_history",
        snowflake_conn_id="jan_airflow_snowflake",
        sql="sql/load_fact_stock_history.sql"
    )

    create_tables >> load_dim_symbol >> load_dim_company
    load_dim_company >> load_dim_date >> load_fact_stock_history
"""
Airflow DAG - Stock Data Warehouse ETL (Cleaned Identifiers)
Team 3
Author: Shuxuan Li
"""

from airflow import DAG
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'team3',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

SNOWFLAKE_CONN_ID = 'jan_airflow_snowflake'

SQL_CLOSE_DIM_COMPANY_PROFILE = """
UPDATE AIRFLOW0105.DEV.DIM_COMPANY_PROFILE_3
SET 
    IS_CURRENT = FALSE,
    END_DATE = CURRENT_DATE()
WHERE IS_CURRENT = TRUE
AND EXISTS (
    SELECT 1 
    FROM US_STOCK_DAILY.DCCM.COMPANY_PROFILE source
    WHERE DIM_COMPANY_PROFILE_3.COMPANY_ID = source.SYMBOL
      AND (
        DIM_COMPANY_PROFILE_3.NAME IS DISTINCT FROM source.COMPANYNAME OR
        DIM_COMPANY_PROFILE_3.INDUSTRY IS DISTINCT FROM source.INDUSTRY OR
        DIM_COMPANY_PROFILE_3.SECTOR IS DISTINCT FROM source.SECTOR OR
        DIM_COMPANY_PROFILE_3.CEO IS DISTINCT FROM source.CEO OR
        DIM_COMPANY_PROFILE_3.WEBSITE IS DISTINCT FROM source.WEBSITE
      )
);
"""

SQL_LOAD_DIM_DATE = """
MERGE INTO AIRFLOW0105.DEV.DIM_DATE_3 target
USING (
    WITH watermark AS (
        SELECT COALESCE(MAX(DATE_KEY), 0) AS MAX_DATE_KEY
        FROM AIRFLOW0105.DEV.DIM_DATE_3
    )
    SELECT DISTINCT
        TO_NUMBER(TO_CHAR(sh.DATE, 'YYYYMMDD')) AS DATE_KEY,
        sh.DATE AS FULL_DATE,
        DAYOFWEEK(sh.DATE) AS DAY_OF_WEEK,
        DAYNAME(sh.DATE) AS DAY_NAME,
        DAY(sh.DATE) AS DAY_OF_MONTH,
        DAYOFYEAR(sh.DATE) AS DAY_OF_YEAR,
        WEEKOFYEAR(sh.DATE) AS WEEK_OF_YEAR,
        MONTH(sh.DATE) AS MONTH_NUMBER,
        MONTHNAME(sh.DATE) AS MONTH_NAME,
        QUARTER(sh.DATE) AS QUARTER,
        YEAR(sh.DATE) AS YEAR,
        TRUE AS IS_TRADING_DAY
    FROM US_STOCK_DAILY.DCCM.STOCK_HISTORY sh
    CROSS JOIN watermark w
    WHERE sh.DATE IS NOT NULL
      AND TO_NUMBER(TO_CHAR(sh.DATE, 'YYYYMMDD')) > w.MAX_DATE_KEY
) source
ON target.DATE_KEY = source.DATE_KEY
WHEN NOT MATCHED THEN
    INSERT (DATE_KEY, FULL_DATE, DAY_OF_WEEK, DAY_NAME, DAY_OF_MONTH, 
            DAY_OF_YEAR, WEEK_OF_YEAR, MONTH_NUMBER, MONTH_NAME, QUARTER, YEAR, IS_TRADING_DAY)
    VALUES (source.DATE_KEY, source.FULL_DATE, source.DAY_OF_WEEK, source.DAY_NAME, 
            source.DAY_OF_MONTH, source.DAY_OF_YEAR, source.WEEK_OF_YEAR, 
            source.MONTH_NUMBER, source.MONTH_NAME, source.QUARTER, source.YEAR, source.IS_TRADING_DAY);
"""

with DAG(
    dag_id='stock_dwh_etl_team3',
    default_args=default_args,
    description='Stock DWH ETL - Fixed Identifiers',
    schedule_interval='@daily',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['team3', 'fix'],
) as dag:

    close_dim_company_profile = SnowflakeOperator(
        task_id='close_dim_company_profile',
        sql=SQL_CLOSE_DIM_COMPANY_PROFILE,
        snowflake_conn_id=SNOWFLAKE_CONN_ID,
    )

    load_dim_date = SnowflakeOperator(
        task_id='load_dim_date',
        sql=SQL_LOAD_DIM_DATE,
        snowflake_conn_id=SNOWFLAKE_CONN_ID,
    )


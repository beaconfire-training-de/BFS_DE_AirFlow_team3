"""
Airflow DAG - Stock Data Warehouse ETL 
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

# --- SQL Definitions ---

SQL_CREATE_FACT_STOCK_HISTORY = """
CREATE TABLE IF NOT EXISTS AIRFLOW0105.DEV.FACT_STOCK_HISTORY_3 (
    FACT_ID NUMBER AUTOINCREMENT PRIMARY KEY,
    COMPANY_ID VARCHAR(64),
    SYMBOL VARCHAR(16) NOT NULL,
    DATE_KEY NUMBER NOT NULL,
    OPEN NUMBER(18,8),
    HIGH NUMBER(18,8),
    LOW NUMBER(18,8),
    CLOSE NUMBER(18,8),
    VOLUME NUMBER(38,8),
    ADJCLOSE NUMBER(38,8),
    DAILY_CHANGE NUMBER(18,8),
    DAILY_CHANGE_PCT NUMBER(18,8),
    MOVING_AVG_7_DAY NUMBER(18,8),
    MOVING_AVG_30_DAY NUMBER(18,8),
    MOVING_AVG_90_DAY NUMBER(18,8),
    BETA NUMBER(18,8),
    MKTCAP NUMBER(38,0),
    LASTDIV NUMBER(18,8),
    RANGE VARCHAR(64),
    DCF NUMBER(18,8),
    DCFDIFF NUMBER(18,8),
    LOAD_TIME TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);
"""

SQL_CLOSE_DIM_COMPANY_PROFILE = """
UPDATE AIRFLOW0105.DEV.DIM_COMPANY_PROFILE_3
SET IS_CURRENT = FALSE,
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

SQL_MERGE_DIM_COMPANY_PROFILE = """
MERGE INTO AIRFLOW0105.DEV.DIM_COMPANY_PROFILE_3 target
USING (
    SELECT DISTINCT
        SYMBOL AS COMPANY_ID,
        COMPANYNAME AS NAME,
        INDUSTRY,
        WEBSITE,
        DESCRIPTION,
        CEO,
        SECTOR,
        TRUE AS IS_CURRENT,
        CURRENT_DATE() AS EFFECTIVE_DATE,
        NULL AS END_DATE
    FROM US_STOCK_DAILY.DCCM.COMPANY_PROFILE
    WHERE SYMBOL IS NOT NULL
) source
ON target.COMPANY_ID = source.COMPANY_ID AND target.IS_CURRENT = TRUE
WHEN NOT MATCHED THEN
    INSERT (COMPANY_ID, NAME, INDUSTRY, WEBSITE, DESCRIPTION, CEO, SECTOR, IS_CURRENT, EFFECTIVE_DATE, END_DATE)
    VALUES (source.COMPANY_ID, source.NAME, source.INDUSTRY, source.WEBSITE, source.DESCRIPTION, source.CEO, source.SECTOR, source.IS_CURRENT, source.EFFECTIVE_DATE, source.END_DATE);
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

SQL_LOAD_FACT_STOCK_HISTORY = """
MERGE INTO AIRFLOW0105.DEV.FACT_STOCK_HISTORY_3 target
USING (
    SELECT
        cp.SYMBOL AS COMPANY_ID,
        sh.SYMBOL,
        TO_NUMBER(TO_CHAR(sh.DATE, 'YYYYMMDD')) AS DATE_KEY,
        sh.OPEN,
        sh.HIGH,
        sh.LOW,
        sh.CLOSE,
        sh.VOLUME,
        sh.ADJCLOSE,
        sh.CLOSE - sh.OPEN AS DAILY_CHANGE,
        CASE WHEN sh.OPEN != 0 THEN (sh.CLOSE - sh.OPEN) / sh.OPEN ELSE NULL END AS DAILY_CHANGE_PCT,
        AVG(sh.CLOSE) OVER (PARTITION BY sh.SYMBOL ORDER BY sh.DATE ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS MOVING_AVG_7_DAY,
        AVG(sh.CLOSE) OVER (PARTITION BY sh.SYMBOL ORDER BY sh.DATE ROWS BETWEEN 29 PRECEDING AND CURRENT ROW) AS MOVING_AVG_30_DAY,
        AVG(sh.CLOSE) OVER (PARTITION BY sh.SYMBOL ORDER BY sh.DATE ROWS BETWEEN 89 PRECEDING AND CURRENT ROW) AS MOVING_AVG_90_DAY,
        cp.BETA,
        cp.MKTCAP,
        cp.LASTDIV,
        cp.RANGE,
        cp.DCF,
        cp.DCFDIFF
    FROM US_STOCK_DAILY.DCCM.STOCK_HISTORY sh
    LEFT JOIN US_STOCK_DAILY.DCCM.COMPANY_PROFILE cp
      ON sh.SYMBOL = cp.SYMBOL
    WHERE sh.DATE IS NOT NULL
) source
ON target.SYMBOL = source.SYMBOL AND target.DATE_KEY = source.DATE_KEY
WHEN NOT MATCHED THEN
    INSERT (
        COMPANY_ID, SYMBOL, DATE_KEY, OPEN, HIGH, LOW, CLOSE, VOLUME, ADJCLOSE,
        DAILY_CHANGE, DAILY_CHANGE_PCT, MOVING_AVG_7_DAY, MOVING_AVG_30_DAY,
        MOVING_AVG_90_DAY, BETA, MKTCAP, LASTDIV, RANGE, DCF, DCFDIFF
    )
    VALUES (
        source.COMPANY_ID, source.SYMBOL, source.DATE_KEY, source.OPEN, source.HIGH, source.LOW, source.CLOSE,
        source.VOLUME, source.ADJCLOSE, source.DAILY_CHANGE, source.DAILY_CHANGE_PCT, source.MOVING_AVG_7_DAY,
        source.MOVING_AVG_30_DAY, source.MOVING_AVG_90_DAY, source.BETA, source.MKTCAP, source.LASTDIV,
        source.RANGE, source.DCF, source.DCFDIFF
    );
"""

with DAG(
    dag_id='stock_dwh_etl_team3',
    default_args=default_args,
    description='Stock DWH ETL - Full Version with Fixes',
    schedule_interval='@daily',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['team3', 'fix'],
) as dag:

    create_fact_stock_history = SnowflakeOperator(
        task_id='create_fact_stock_history',
        sql=SQL_CREATE_FACT_STOCK_HISTORY,
        snowflake_conn_id=SNOWFLAKE_CONN_ID,
    )

    close_dim_company_profile = SnowflakeOperator(
        task_id='close_dim_company_profile',
        sql=SQL_CLOSE_DIM_COMPANY_PROFILE,
        snowflake_conn_id=SNOWFLAKE_CONN_ID,
    )

    load_dim_company_profile = SnowflakeOperator(
        task_id='load_dim_company_profile',
        sql=SQL_MERGE_DIM_COMPANY_PROFILE,
        snowflake_conn_id=SNOWFLAKE_CONN_ID,
    )

    load_dim_date = SnowflakeOperator(
        task_id='load_dim_date',
        sql=SQL_LOAD_DIM_DATE,
        snowflake_conn_id=SNOWFLAKE_CONN_ID,
    )

    load_fact_stock_history = SnowflakeOperator(
        task_id='load_fact_stock_history',
        sql=SQL_LOAD_FACT_STOCK_HISTORY,
        snowflake_conn_id=SNOWFLAKE_CONN_ID,
    )

    create_fact_stock_history >> close_dim_company_profile >> load_dim_company_profile
    load_dim_company_profile >> load_dim_date >> load_fact_stock_history

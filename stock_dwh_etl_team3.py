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

# --- SQL Definitions ---

SQL_CREATE_DIM_COMPANY_PROFILE = """
CREATE TABLE IF NOT EXISTS AIRFLOW0105.DEV.DIM_COMPANY_PROFILE_3 (
    COMPANY_ID VARCHAR(64) PRIMARY KEY,
    NAME VARCHAR(512),
    INDUSTRY VARCHAR(64),
    WEBSITE VARCHAR(64),
    DESCRIPTION VARCHAR(2048),
    CEO VARCHAR(64),
    SECTOR VARCHAR(64),
    IS_CURRENT BOOLEAN DEFAULT TRUE,
    EFFECTIVE_DATE DATE DEFAULT CURRENT_DATE(),
    END_DATE DATE DEFAULT NULL
);
"""

SQL_CREATE_DIM_SYMBOL = """
CREATE TABLE IF NOT EXISTS AIRFLOW0105.DEV.DIM_SYMBOL_3 (
    SYMBOL VARCHAR(16) PRIMARY KEY,
    NAME VARCHAR(256),
    EXCHANGE VARCHAR(64)
);
"""

SQL_CREATE_DIM_DATE = """
CREATE TABLE IF NOT EXISTS AIRFLOW0105.DEV.DIM_DATE_3 (
    DATE_KEY NUMBER PRIMARY KEY,
    FULL_DATE DATE NOT NULL,
    DAY_OF_WEEK NUMBER,
    DAY_NAME VARCHAR(10),
    DAY_OF_MONTH NUMBER,
    DAY_OF_YEAR NUMBER,
    WEEK_OF_YEAR NUMBER,
    MONTH_NUMBER NUMBER,
    MONTH_NAME VARCHAR(10),
    QUARTER NUMBER,
    YEAR NUMBER,
    IS_TRADING_DAY BOOLEAN DEFAULT TRUE
);
"""

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
    LOAD_TIME TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    FOREIGN KEY (SYMBOL) REFERENCES AIRFLOW0105.DEV.DIM_SYMBOL_3(SYMBOL),
    FOREIGN KEY (DATE_KEY) REFERENCES AIRFLOW0105.DEV.DIM_DATE_3(DATE_KEY),
    FOREIGN KEY (COMPANY_ID) REFERENCES AIRFLOW0105.DEV.DIM_COMPANY_PROFILE_3(COMPANY_ID)
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

SQL_LOAD_DIM_SYMBOL = """
MERGE INTO AIRFLOW0105.DEV.DIM_SYMBOL_3 target
USING (
    SELECT SYMBOL, NAME, EXCHANGE
    FROM US_STOCK_DAILY.DCCM.SYMBOLS
    WHERE SYMBOL IS NOT NULL
) source
ON target.SYMBOL = source.SYMBOL
WHEN NOT MATCHED THEN
    INSERT (SYMBOL, NAME, EXCHANGE)
    VALUES (source.SYMBOL, source.NAME, source.EXCHANGE)
WHEN MATCHED THEN
    UPDATE SET
        target.NAME = source.NAME,
        target.EXCHANGE = source.EXCHANGE;
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

SQL_VALIDATE_ROW_COUNTS = """
SELECT 'DIM_COMPANY_PROFILE_3' AS TABLE_NAME, COUNT(*) FROM AIRFLOW0105.DEV.DIM_COMPANY_PROFILE_3
UNION ALL
SELECT 'DIM_SYMBOL_3', COUNT(*) FROM AIRFLOW0105.DEV.DIM_SYMBOL_3
UNION ALL
SELECT 'DIM_DATE_3', COUNT(*) FROM AIRFLOW0105.DEV.DIM_DATE_3
UNION ALL
SELECT 'FACT_STOCK_HISTORY_3', COUNT(*) FROM AIRFLOW0105.DEV.FACT_STOCK_HISTORY_3;
"""

SQL_VALIDATE_NULLS = """
SELECT 'FACT with NULL SYMBOL', COUNT(*) FROM AIRFLOW0105.DEV.FACT_STOCK_HISTORY_3 WHERE SYMBOL IS NULL
UNION ALL
SELECT 'FACT with NULL DATE_KEY', COUNT(*) FROM AIRFLOW0105.DEV.FACT_STOCK_HISTORY_3 WHERE DATE_KEY IS NULL;
"""

SQL_VALIDATE_REFERENTIAL_INTEGRITY = """
SELECT 'Orphan SYMBOL in FACT', COUNT(*)
FROM AIRFLOW0105.DEV.FACT_STOCK_HISTORY_3 f
LEFT JOIN AIRFLOW0105.DEV.DIM_SYMBOL_3 s ON f.SYMBOL = s.SYMBOL
WHERE s.SYMBOL IS NULL
UNION ALL
SELECT 'Orphan DATE_KEY in FACT', COUNT(*)
FROM AIRFLOW0105.DEV.FACT_STOCK_HISTORY_3 f
LEFT JOIN AIRFLOW0105.DEV.DIM_DATE_3 d ON f.DATE_KEY = d.DATE_KEY
WHERE d.DATE_KEY IS NULL;
"""

# --- DAG Definition ---

with DAG(
    dag_id='stock_dwh_etl_team3',
    default_args=default_args,
    description='Stock DWH ETL - Clean Identifiers',
    schedule_interval='@daily',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['team3'],
) as dag:

    create_dim_company_profile = SnowflakeOperator(
        task_id='create_dim_company_profile',
        sql=SQL_CREATE_DIM_COMPANY_PROFILE,
        snowflake_conn_id=SNOWFLAKE_CONN_ID,
    )

    create_dim_symbol = SnowflakeOperator(
        task_id='create_dim_symbol',
        sql=SQL_CREATE_DIM_SYMBOL,
        snowflake_conn_id=SNOWFLAKE_CONN_ID,
    )

    create_dim_date = SnowflakeOperator(
        task_id='create_dim_date',
        sql=SQL_CREATE_DIM_DATE,
        snowflake_conn_id=SNOWFLAKE_CONN_ID,
    )

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

    merge_dim_company_profile = SnowflakeOperator(
        task_id='merge_dim_company_profile',
        sql=SQL_MERGE_DIM_COMPANY_PROFILE,
        snowflake_conn_id=SNOWFLAKE_CONN_ID,
    )

    load_dim_symbol = SnowflakeOperator(
        task_id='load_dim_symbol',
        sql=SQL_LOAD_DIM_SYMBOL,
        snowflake_conn_id=SNOWFLAKE_CONN_ID,
    )

    load_dim_date = SnowflakeOperator(
        task_id='load_dim_date',
        sql=SQL_LOAD_DIM_DATE,
        snowflake_conn_id=SNOWFLAKE_CONN_ID,
    )

    validate_row_counts = SnowflakeOperator(
        task_id='validate_row_counts',
        sql=SQL_VALIDATE_ROW_COUNTS,
        snowflake_conn_id=SNOWFLAKE_CONN_ID,
    )

    validate_nulls = SnowflakeOperator(
        task_id='validate_nulls',
        sql=SQL_VALIDATE_NULLS,
        snowflake_conn_id=SNOWFLAKE_CONN_ID,
    )

    validate_referential_integrity = SnowflakeOperator(
        task_id='validate_referential_integrity',
        sql=SQL_VALIDATE_REFERENTIAL_INTEGRITY,
        snowflake_conn_id=SNOWFLAKE_CONN_ID,
    )

    [create_dim_company_profile, create_dim_symbol, create_dim_date] >> create_fact_stock_history
    create_dim_company_profile >> close_dim_company_profile >> merge_dim_company_profile
    create_dim_symbol >> load_dim_symbol
    create_dim_date >> load_dim_date
    create_fact_stock_history >> load_fact_stock_history
    [merge_dim_company_profile, load_dim_symbol, load_dim_date] >> load_fact_stock_history
    load_fact_stock_history >> [validate_row_counts, validate_nulls, validate_referential_integrity]

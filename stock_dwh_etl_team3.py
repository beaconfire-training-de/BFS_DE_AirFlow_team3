"""
Airflow DAG - Stock Data Warehouse ETL
Team 3
Author: Shuxuan Li
Time: 18:41 PST
"""

from airflow import DAG
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from datetime import datetime, timedelta

# DAG settings
default_args = {
    'owner': 'team3',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

SNOWFLAKE_CONN_ID = 'jan_airflow_snowflake'

# DDL SQL

# DIM_COMPANY_PROFILE_3 (SCD fields)
SQL_CREATE_DIM_COMPANY_PROFILE = """
CREATE TABLE IF NOT EXISTS AIRFLOW0105.DEV.DIM_COMPANY_PROFILE_3 (
    COMPANY_ID VARCHAR(32),
    SYMBOL VARCHAR(16),
    COMPANY_NAME VARCHAR(512),
    INDUSTRY VARCHAR(64),
    WEBSITE VARCHAR(64),
    DESCRIPTION VARCHAR(2048),
    CEO VARCHAR(64),
    SECTOR VARCHAR(64),
    IS_CURRENT BOOLEAN DEFAULT TRUE,
    EFFECTIVE_DATE TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    END_DATE TIMESTAMP_NTZ DEFAULT NULL
);
"""

# DIM_SYMBOL_3
SQL_CREATE_DIM_SYMBOL = """
CREATE TABLE IF NOT EXISTS AIRFLOW0105.DEV.DIM_SYMBOL_3 (
    SYMBOL VARCHAR(16) PRIMARY KEY,
    "NAME" VARCHAR(256),
    EXCHANGE VARCHAR(64)
);
"""

# DIM_DATE_3
SQL_CREATE_DIM_DATE = """
CREATE TABLE IF NOT EXISTS AIRFLOW0105.DEV.DIM_DATE_3 (
    DATE_KEY NUMBER PRIMARY KEY,
    FULL_DATE DATE NOT NULL,
    DAY_OF_WEEK NUMBER,
    DAY_OF_WEEK_NAME VARCHAR(16777216),
    DAY_OF_MONTH NUMBER,
    DAY_OF_YEAR NUMBER,
    WEEK_OF_YEAR NUMBER,
    MONTH_NUMBER NUMBER,
    MONTH_NAME VARCHAR(16777216),
    QUARTER NUMBER,
    YEAR NUMBER,
    IS_TRADING_DAY BOOLEAN DEFAULT TRUE
);
"""

# FACT_STOCK_HISTORY_3 (includes calculated metrics)
SQL_CREATE_FACT_STOCK_HISTORY = """
CREATE TABLE IF NOT EXISTS AIRFLOW0105.DEV.FACT_STOCK_HISTORY_3 (
    FACT_ID NUMBER AUTOINCREMENT PRIMARY KEY,
    COMPANY_ID VARCHAR(64),
    SYMBOL VARCHAR(16) NOT NULL,
    DATE_KEY NUMBER NOT NULL,
    "OPEN" NUMBER(18,8),
    HIGH NUMBER(18,8),
    LOW NUMBER(18,8),
    "CLOSE" NUMBER(18,8),
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
    "RANGE" VARCHAR(64),
    DCF NUMBER(18,8),
    DCFDIFF NUMBER(18,8),
    LOAD_TIME TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    FOREIGN KEY (SYMBOL) REFERENCES AIRFLOW0105.DEV.DIM_SYMBOL_3(SYMBOL),
    FOREIGN KEY (DATE_KEY) REFERENCES AIRFLOW0105.DEV.DIM_DATE_3(DATE_KEY),
    FOREIGN KEY (COMPANY_ID) REFERENCES AIRFLOW0105.DEV.DIM_COMPANY_PROFILE_3(COMPANY_ID)
);
"""

# Keep FACT schema aligned when table already existed before this DAG version.
SQL_SYNC_FACT_STOCK_HISTORY_COLUMNS = [
    'ALTER TABLE AIRFLOW0105.DEV.FACT_STOCK_HISTORY_3 ADD COLUMN IF NOT EXISTS COMPANY_ID VARCHAR(64)',
    'ALTER TABLE AIRFLOW0105.DEV.FACT_STOCK_HISTORY_3 ADD COLUMN IF NOT EXISTS SYMBOL VARCHAR(16)',
    'ALTER TABLE AIRFLOW0105.DEV.FACT_STOCK_HISTORY_3 ADD COLUMN IF NOT EXISTS DATE_KEY NUMBER(38,0)',
    'ALTER TABLE AIRFLOW0105.DEV.FACT_STOCK_HISTORY_3 ADD COLUMN IF NOT EXISTS "OPEN" NUMBER(18,8)',
    'ALTER TABLE AIRFLOW0105.DEV.FACT_STOCK_HISTORY_3 ADD COLUMN IF NOT EXISTS HIGH NUMBER(18,8)',
    'ALTER TABLE AIRFLOW0105.DEV.FACT_STOCK_HISTORY_3 ADD COLUMN IF NOT EXISTS LOW NUMBER(18,8)',
    'ALTER TABLE AIRFLOW0105.DEV.FACT_STOCK_HISTORY_3 ADD COLUMN IF NOT EXISTS "CLOSE" NUMBER(18,8)',
    'ALTER TABLE AIRFLOW0105.DEV.FACT_STOCK_HISTORY_3 ADD COLUMN IF NOT EXISTS VOLUME NUMBER(38,8)',
    'ALTER TABLE AIRFLOW0105.DEV.FACT_STOCK_HISTORY_3 ADD COLUMN IF NOT EXISTS ADJCLOSE NUMBER(38,8)',
    'ALTER TABLE AIRFLOW0105.DEV.FACT_STOCK_HISTORY_3 ADD COLUMN IF NOT EXISTS DAILY_CHANGE NUMBER(18,8)',
    'ALTER TABLE AIRFLOW0105.DEV.FACT_STOCK_HISTORY_3 ADD COLUMN IF NOT EXISTS DAILY_CHANGE_PCT NUMBER(18,8)',
    'ALTER TABLE AIRFLOW0105.DEV.FACT_STOCK_HISTORY_3 ADD COLUMN IF NOT EXISTS MOVING_AVG_7_DAY NUMBER(18,8)',
    'ALTER TABLE AIRFLOW0105.DEV.FACT_STOCK_HISTORY_3 ADD COLUMN IF NOT EXISTS MOVING_AVG_30_DAY NUMBER(18,8)',
    'ALTER TABLE AIRFLOW0105.DEV.FACT_STOCK_HISTORY_3 ADD COLUMN IF NOT EXISTS MOVING_AVG_90_DAY NUMBER(18,8)',
    'ALTER TABLE AIRFLOW0105.DEV.FACT_STOCK_HISTORY_3 ADD COLUMN IF NOT EXISTS BETA NUMBER(18,8)',
    'ALTER TABLE AIRFLOW0105.DEV.FACT_STOCK_HISTORY_3 ADD COLUMN IF NOT EXISTS MKTCAP NUMBER(38,0)',
    'ALTER TABLE AIRFLOW0105.DEV.FACT_STOCK_HISTORY_3 ADD COLUMN IF NOT EXISTS LASTDIV NUMBER(18,8)',
    'ALTER TABLE AIRFLOW0105.DEV.FACT_STOCK_HISTORY_3 ADD COLUMN IF NOT EXISTS "RANGE" VARCHAR(64)',
    'ALTER TABLE AIRFLOW0105.DEV.FACT_STOCK_HISTORY_3 ADD COLUMN IF NOT EXISTS DCF NUMBER(18,8)',
    'ALTER TABLE AIRFLOW0105.DEV.FACT_STOCK_HISTORY_3 ADD COLUMN IF NOT EXISTS DCFDIFF NUMBER(18,8)',
    'ALTER TABLE AIRFLOW0105.DEV.FACT_STOCK_HISTORY_3 ADD COLUMN IF NOT EXISTS LOAD_TIME TIMESTAMP_NTZ',
]

# Load SQL

# Step 1 for DIM_COMPANY_PROFILE_3 SCD2: close current row if source changed
SQL_CLOSE_DIM_COMPANY_PROFILE = """
UPDATE AIRFLOW0105.DEV.DIM_COMPANY_PROFILE_3 AS target
SET 
    IS_CURRENT = FALSE,
    END_DATE = CURRENT_TIMESTAMP()
FROM US_STOCK_DAILY.DCCM.COMPANY_PROFILE AS source
WHERE target.IS_CURRENT = TRUE
  AND target.SYMBOL = source.SYMBOL
  AND (
      target.COMPANY_NAME IS DISTINCT FROM source.COMPANYNAME OR
      target.INDUSTRY IS DISTINCT FROM source.INDUSTRY OR
      target.SECTOR IS DISTINCT FROM source.SECTOR OR
      target.CEO IS DISTINCT FROM source.CEO OR
      target.WEBSITE IS DISTINCT FROM source.WEBSITE OR
      target.DESCRIPTION IS DISTINCT FROM source.DESCRIPTION
  );
"""

# Step 2 for DIM_COMPANY_PROFILE_3 SCD2: insert new company rows and new versions
SQL_MERGE_DIM_COMPANY_PROFILE = """
MERGE INTO AIRFLOW0105.DEV.DIM_COMPANY_PROFILE_3 AS target
USING (
    SELECT DISTINCT
        SYMBOL AS COMPANY_ID,
        SYMBOL,
        COMPANYNAME AS COMPANY_NAME,
        INDUSTRY,
        WEBSITE,
        DESCRIPTION,
        CEO,
        SECTOR,
        TRUE AS IS_CURRENT,
        CURRENT_TIMESTAMP() AS EFFECTIVE_DATE,
        NULL AS END_DATE
    FROM US_STOCK_DAILY.DCCM.COMPANY_PROFILE
    WHERE SYMBOL IS NOT NULL
) AS source
ON target.SYMBOL = source.SYMBOL AND target.IS_CURRENT = TRUE
WHEN NOT MATCHED THEN
    INSERT (COMPANY_ID, SYMBOL, COMPANY_NAME, INDUSTRY, WEBSITE, DESCRIPTION, CEO, SECTOR, 
            IS_CURRENT, EFFECTIVE_DATE, END_DATE)
    VALUES (source.COMPANY_ID, source.SYMBOL, source.COMPANY_NAME, source.INDUSTRY, source.WEBSITE, 
            source.DESCRIPTION, source.CEO, source.SECTOR,
            source.IS_CURRENT, source.EFFECTIVE_DATE, source.END_DATE);
"""

# Load DIM_SYMBOL_3
SQL_LOAD_DIM_SYMBOL = """
MERGE INTO AIRFLOW0105.DEV.DIM_SYMBOL_3 AS target
USING (
    SELECT 
        SYMBOL,
        "NAME",
        EXCHANGE
    FROM US_STOCK_DAILY.DCCM.SYMBOLS
    WHERE SYMBOL IS NOT NULL
) AS source
ON target.SYMBOL = source.SYMBOL
WHEN NOT MATCHED THEN
    INSERT (SYMBOL, "NAME", EXCHANGE)
    VALUES (source.SYMBOL, source."NAME", source.EXCHANGE)
WHEN MATCHED THEN
    UPDATE SET
        target."NAME" = source."NAME",
        target.EXCHANGE = source.EXCHANGE;
"""

# Load DIM_DATE_3
SQL_LOAD_DIM_DATE = """
MERGE INTO AIRFLOW0105.DEV.DIM_DATE_3 AS target
USING (
    WITH watermark AS (
        SELECT COALESCE(MAX(DATE_KEY), 0) AS MAX_DATE_KEY
        FROM AIRFLOW0105.DEV.DIM_DATE_3
    )
    SELECT DISTINCT
        TO_NUMBER(TO_CHAR(sh.DATE, 'YYYYMMDD')) AS DATE_KEY,
        sh.DATE AS FULL_DATE,
        DAYOFWEEK(sh.DATE) AS DAY_OF_WEEK,
        DAYNAME(sh.DATE) AS DAY_OF_WEEK_NAME,
        DAY(sh.DATE) AS DAY_OF_MONTH,
        DAYOFYEAR(sh.DATE) AS DAY_OF_YEAR,
        WEEKOFYEAR(sh.DATE) AS WEEK_OF_YEAR,
        MONTH(sh.DATE) AS MONTH_NUMBER,
        MONTHNAME(sh.DATE) AS MONTH_NAME,
        QUARTER(sh.DATE) AS CAL_QUARTER,
        YEAR(sh.DATE) AS CAL_YEAR,
        TRUE AS IS_TRADING_DAY
    FROM US_STOCK_DAILY.DCCM.STOCK_HISTORY sh
    CROSS JOIN watermark w
    WHERE sh.DATE IS NOT NULL
        AND TO_NUMBER(TO_CHAR(sh.DATE, 'YYYYMMDD')) > w.MAX_DATE_KEY
) AS source
ON target.DATE_KEY = source.DATE_KEY
WHEN NOT MATCHED THEN
    INSERT (DATE_KEY, FULL_DATE, DAY_OF_WEEK, DAY_OF_WEEK_NAME, DAY_OF_MONTH, 
            DAY_OF_YEAR, WEEK_OF_YEAR, MONTH_NUMBER, MONTH_NAME, QUARTER, YEAR, IS_TRADING_DAY)
    VALUES (source.DATE_KEY, source.FULL_DATE, source.DAY_OF_WEEK, source.DAY_OF_WEEK_NAME, 
            source.DAY_OF_MONTH, source.DAY_OF_YEAR, source.WEEK_OF_YEAR, 
            source.MONTH_NUMBER, source.MONTH_NAME, source.CAL_QUARTER, source.CAL_YEAR, source.IS_TRADING_DAY);
"""

# Load FACT_STOCK_HISTORY_3 with calculated fields
SQL_LOAD_FACT_STOCK_HISTORY = """
MERGE INTO AIRFLOW0105.DEV.FACT_STOCK_HISTORY_3 AS target
USING (
    WITH watermark AS (
        SELECT COALESCE(MAX(DATE_KEY), 0) AS MAX_DATE_KEY
        FROM AIRFLOW0105.DEV.FACT_STOCK_HISTORY_3
    ),
    stock_for_calc AS (
        SELECT 
            sh.SYMBOL,
            TO_NUMBER(TO_CHAR(sh.DATE, 'YYYYMMDD')) AS DATE_KEY,
            cp.SYMBOL AS COMPANY_ID,
            sh."OPEN",
            sh.HIGH,
            sh.LOW,
            sh."CLOSE",
            sh.VOLUME,
            sh.ADJCLOSE,
            cp.BETA,
            cp.MKTCAP,
            cp.LASTDIV,
            cp."RANGE",
            cp.DCF,
            cp.DCFDIFF
        FROM US_STOCK_DAILY.DCCM.STOCK_HISTORY sh
        LEFT JOIN US_STOCK_DAILY.DCCM.COMPANY_PROFILE cp 
            ON sh.SYMBOL = cp.SYMBOL
        CROSS JOIN watermark w
        WHERE sh.SYMBOL IS NOT NULL
            AND sh.DATE IS NOT NULL
            AND TO_NUMBER(TO_CHAR(sh.DATE, 'YYYYMMDD')) >= GREATEST(w.MAX_DATE_KEY - 178, 0)
    ),
    stock_with_calc AS (
        SELECT
            sfc.SYMBOL,
            sfc.DATE_KEY,
            sfc.COMPANY_ID,
            sfc."OPEN",
            sfc.HIGH,
            sfc.LOW,
            sfc."CLOSE",
            sfc.VOLUME,
            sfc.ADJCLOSE,
            -- Daily Change (today's close - yesterday's close)
            sfc."CLOSE" - LAG(sfc."CLOSE") OVER (PARTITION BY sfc.SYMBOL ORDER BY sfc.DATE_KEY) AS DAILY_CHANGE,
            -- Daily Change Percentage
            CASE
                WHEN LAG(sfc."CLOSE") OVER (PARTITION BY sfc.SYMBOL ORDER BY sfc.DATE_KEY) != 0
                THEN ((sfc."CLOSE" - LAG(sfc."CLOSE") OVER (PARTITION BY sfc.SYMBOL ORDER BY sfc.DATE_KEY))
                      / LAG(sfc."CLOSE") OVER (PARTITION BY sfc.SYMBOL ORDER BY sfc.DATE_KEY)) * 100
                ELSE 0
            END AS DAILY_CHANGE_PCT,
            -- Moving Averages
            AVG(sfc."CLOSE") OVER (PARTITION BY sfc.SYMBOL ORDER BY sfc.DATE_KEY ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS MOVING_AVG_7_DAY,
            AVG(sfc."CLOSE") OVER (PARTITION BY sfc.SYMBOL ORDER BY sfc.DATE_KEY ROWS BETWEEN 29 PRECEDING AND CURRENT ROW) AS MOVING_AVG_30_DAY,
            AVG(sfc."CLOSE") OVER (PARTITION BY sfc.SYMBOL ORDER BY sfc.DATE_KEY ROWS BETWEEN 89 PRECEDING AND CURRENT ROW) AS MOVING_AVG_90_DAY,
            sfc.BETA,
            sfc.MKTCAP,
            sfc.LASTDIV,
            sfc."RANGE",
            sfc.DCF,
            sfc.DCFDIFF
        FROM stock_for_calc sfc
    )
    SELECT swc.*
    FROM stock_with_calc swc
    CROSS JOIN watermark w
    WHERE swc.DATE_KEY >= GREATEST(w.MAX_DATE_KEY - 89, 0)
) AS source
ON target.SYMBOL = source.SYMBOL AND target.DATE_KEY = source.DATE_KEY
WHEN NOT MATCHED THEN
    INSERT (COMPANY_ID, SYMBOL, DATE_KEY, "OPEN", HIGH, LOW, "CLOSE", VOLUME, ADJCLOSE,
            DAILY_CHANGE, DAILY_CHANGE_PCT, MOVING_AVG_7_DAY, MOVING_AVG_30_DAY, MOVING_AVG_90_DAY,
            BETA, MKTCAP, LASTDIV, "RANGE", DCF, DCFDIFF)
    VALUES (source.COMPANY_ID, source.SYMBOL, source.DATE_KEY, source."OPEN", source.HIGH, 
            source.LOW, source."CLOSE", source.VOLUME, source.ADJCLOSE,
            source.DAILY_CHANGE, source.DAILY_CHANGE_PCT, 
            source.MOVING_AVG_7_DAY, source.MOVING_AVG_30_DAY, source.MOVING_AVG_90_DAY,
            source.BETA, source.MKTCAP, source.LASTDIV, source."RANGE", source.DCF, source.DCFDIFF)
WHEN MATCHED AND (
    target.COMPANY_ID IS DISTINCT FROM source.COMPANY_ID OR
    target."OPEN" IS DISTINCT FROM source."OPEN" OR
    target.HIGH IS DISTINCT FROM source.HIGH OR
    target.LOW IS DISTINCT FROM source.LOW OR
    target."CLOSE" IS DISTINCT FROM source."CLOSE" OR
    target.VOLUME IS DISTINCT FROM source.VOLUME OR
    target.ADJCLOSE IS DISTINCT FROM source.ADJCLOSE OR
    target.DAILY_CHANGE IS DISTINCT FROM source.DAILY_CHANGE OR
    target.DAILY_CHANGE_PCT IS DISTINCT FROM source.DAILY_CHANGE_PCT OR
    target.MOVING_AVG_7_DAY IS DISTINCT FROM source.MOVING_AVG_7_DAY OR
    target.MOVING_AVG_30_DAY IS DISTINCT FROM source.MOVING_AVG_30_DAY OR
    target.MOVING_AVG_90_DAY IS DISTINCT FROM source.MOVING_AVG_90_DAY OR
    target.BETA IS DISTINCT FROM source.BETA OR
    target.MKTCAP IS DISTINCT FROM source.MKTCAP OR
    target.LASTDIV IS DISTINCT FROM source.LASTDIV OR
    target."RANGE" IS DISTINCT FROM source."RANGE" OR
    target.DCF IS DISTINCT FROM source.DCF OR
    target.DCFDIFF IS DISTINCT FROM source.DCFDIFF
) THEN
    UPDATE SET
        target.COMPANY_ID = source.COMPANY_ID,
        target."OPEN" = source."OPEN",
        target.HIGH = source.HIGH,
        target.LOW = source.LOW,
        target."CLOSE" = source."CLOSE",
        target.VOLUME = source.VOLUME,
        target.ADJCLOSE = source.ADJCLOSE,
        target.DAILY_CHANGE = source.DAILY_CHANGE,
        target.DAILY_CHANGE_PCT = source.DAILY_CHANGE_PCT,
        target.MOVING_AVG_7_DAY = source.MOVING_AVG_7_DAY,
        target.MOVING_AVG_30_DAY = source.MOVING_AVG_30_DAY,
        target.MOVING_AVG_90_DAY = source.MOVING_AVG_90_DAY,
        target.BETA = source.BETA,
        target.MKTCAP = source.MKTCAP,
        target.LASTDIV = source.LASTDIV,
        target."RANGE" = source."RANGE",
        target.DCF = source.DCF,
        target.DCFDIFF = source.DCFDIFF,
        target.LOAD_TIME = CURRENT_TIMESTAMP();
"""

# Validation SQL

SQL_VALIDATE_ROW_COUNTS = """
SELECT 'DIM_COMPANY_PROFILE_3' AS TABLE_NAME, COUNT(*) AS ROW_COUNT FROM AIRFLOW0105.DEV.DIM_COMPANY_PROFILE_3
UNION ALL
SELECT 'DIM_SYMBOL_3', COUNT(*) FROM AIRFLOW0105.DEV.DIM_SYMBOL_3
UNION ALL
SELECT 'DIM_DATE_3', COUNT(*) FROM AIRFLOW0105.DEV.DIM_DATE_3
UNION ALL
SELECT 'FACT_STOCK_HISTORY_3', COUNT(*) FROM AIRFLOW0105.DEV.FACT_STOCK_HISTORY_3;
"""

SQL_VALIDATE_NULLS = """
SELECT 'FACT with NULL SYMBOL' AS CHECK_NAME, COUNT(*) AS COUNT 
FROM AIRFLOW0105.DEV.FACT_STOCK_HISTORY_3 WHERE SYMBOL IS NULL
UNION ALL
SELECT 'FACT with NULL DATE_KEY', COUNT(*) 
FROM AIRFLOW0105.DEV.FACT_STOCK_HISTORY_3 WHERE DATE_KEY IS NULL;
"""

SQL_VALIDATE_REFERENTIAL_INTEGRITY = """
SELECT 'Orphan SYMBOL in FACT' AS CHECK_NAME, COUNT(*) AS COUNT
FROM AIRFLOW0105.DEV.FACT_STOCK_HISTORY_3 f
LEFT JOIN AIRFLOW0105.DEV.DIM_SYMBOL_3 s ON f.SYMBOL = s.SYMBOL
WHERE s.SYMBOL IS NULL
UNION ALL
SELECT 'Orphan DATE_KEY in FACT', COUNT(*)
FROM AIRFLOW0105.DEV.FACT_STOCK_HISTORY_3 f
LEFT JOIN AIRFLOW0105.DEV.DIM_DATE_3 d ON f.DATE_KEY = d.DATE_KEY
WHERE d.DATE_KEY IS NULL;
"""

# DAG definition

with DAG(
    dag_id='stock_dwh_etl_team3',
    default_args=default_args,
    description='Stock Data Warehouse ETL - Team 3 (with SCD and Calculated Fields)',
    schedule_interval='@daily',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['team3', 'stock', 'dwh', 'scd'],
) as dag:

    # Create target tables
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

    # Load dimension tables
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

    sync_fact_stock_history_columns = SnowflakeOperator(
        task_id='sync_fact_stock_history_columns',
        sql=SQL_SYNC_FACT_STOCK_HISTORY_COLUMNS,
        snowflake_conn_id=SNOWFLAKE_CONN_ID,
    )

    # Load fact table
    load_fact_stock_history = SnowflakeOperator(
        task_id='load_fact_stock_history',
        sql=SQL_LOAD_FACT_STOCK_HISTORY,
        snowflake_conn_id=SNOWFLAKE_CONN_ID,
    )

    # Validation checks
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

    # Task dependencies
    # Create all dimension tables first, then fact table
    [create_dim_company_profile, create_dim_symbol, create_dim_date] >> create_fact_stock_history
    
    # Load dimensions after tables are created
    create_dim_company_profile >> close_dim_company_profile >> merge_dim_company_profile
    create_dim_symbol >> load_dim_symbol
    create_dim_date >> load_dim_date
    
    # Load fact table after all dimensions are loaded
    create_fact_stock_history >> sync_fact_stock_history_columns >> load_fact_stock_history
    [merge_dim_company_profile, load_dim_symbol, load_dim_date] >> load_fact_stock_history
    
    # Run validation after fact table is loaded
    load_fact_stock_history >> [validate_row_counts, validate_nulls, validate_referential_integrity]

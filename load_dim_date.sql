MERGE INTO AIRFLOW0105.DEV.DIM_DATE tgt
USING (
    SELECT DISTINCT
        TO_NUMBER(TO_CHAR(date, 'YYYYMMDD')) AS date_key,
        date AS full_date,

        DAYOFWEEK(date) AS day_of_week,
        DAYNAME(date) AS day_name,
        DAYOFYEAR(date) AS day_of_year,
        DAY(date) AS day_of_month,
        WEEKOFYEAR(date) AS week_of_year,

        MONTH(date) AS month_number,
        MONTHNAME(date) AS month_name,
        QUARTER(date) AS quarter,
        YEAR(date) AS year,

        CASE
            WHEN DAYOFWEEK(date) IN (1, 7) THEN FALSE
            ELSE TRUE
        END AS is_trading_day

    FROM US_STOCK_DAILY.DCCM.Stock_History
) src
ON tgt.date_key = src.date_key

WHEN NOT MATCHED THEN
INSERT (
    date_key,
    full_date,
    day_of_week,
    day_name,
    day_of_year,
    day_of_month,
    week_of_year,
    month_number,
    month_name,
    quarter,
    year,
    is_trading_day
)
VALUES (
    src.date_key,
    src.full_date,
    src.day_of_week,
    src.day_name,
    src.day_of_year,
    src.day_of_month,
    src.week_of_year,
    src.month_number,
    src.month_name,
    src.quarter,
    src.year,
    src.is_trading_day
);

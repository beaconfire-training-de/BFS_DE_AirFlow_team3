CREATE TABLE IF NOT EXISTS AIRFLOW0105.DEV.DIM_SYMBOL_BRANDON (
    symbol VARCHAR(16) PRIMARY KEY,
    name VARCHAR(256),
    exchange VARCHAR(64)
);

CREATE TABLE IF NOT EXISTS AIRFLOW0105.DEV.DIM_COMPANY_PROFILE_BRANDON (
    company_id VARCHAR(64) PRIMARY KEY,
    name VARCHAR(512),
    industry VARCHAR(64),
    website VARCHAR(64),
    description VARCHAR(2048),
    ceo VARCHAR(64),
    sector VARCHAR(64),

    is_current BOOLEAN,
    effective_date DATE,
    end_date DATE
);

CREATE TABLE IF NOT EXISTS AIRFLOW0105.DEV.DIM_DATE_BRANDON (
    date_key NUMBER PRIMARY KEY,
    full_date DATE,

    day_of_week NUMBER,
    day_name VARCHAR(10),
    day_of_year NUMBER,
    day_of_month NUMBER,
    week_of_year NUMBER,

    month_number NUMBER,
    month_name VARCHAR(10),
    quarter NUMBER,
    year NUMBER,

    is_trading_day BOOLEAN
);

CREATE TABLE IF NOT EXISTS AIRFLOW0105.DEV.FACT_STOCK_HISTORY_BRANDON (
    field VARCHAR(64) PRIMARY KEY,

    company_id VARCHAR(64),
    symbol VARCHAR(16),
    date DATE,

    open NUMBER(18,8),
    high NUMBER(18,8),
    low NUMBER(18,8),
    close NUMBER(18,8),
    volume NUMBER(38,0),
    adjclose NUMBER(18,8),

    daily_change NUMBER(18,8),
    daily_change_pct NUMBER(18,8),

    moving_avg_7_day NUMBER(18,8),
    moving_avg_30_day NUMBER(18,8),
    moving_avg_90_day NUMBER(18,8),

    beta NUMBER(18,8),
    mktcap NUMBER(38,0),
    lastdiv NUMBER(18,8),
    range VARCHAR(64),
    dcf NUMBER(18,8),
    dcfdiff NUMBER(18,8),

    CONSTRAINT fk_company
        FOREIGN KEY (company_id)
        REFERENCES AIRFLOW0105.DEV.DIM_COMPANY_PROFILE_BRANDON(company_id),

    CONSTRAINT fk_symbol
        FOREIGN KEY (symbol)
        REFERENCES AIRFLOW0105.DEV.DIM_SYMBOL_BRANDON(symbol)
);

DROP TABLE AIRFLOW0105.DEV.FACT_STOCK_HISTORY_BRANDON;

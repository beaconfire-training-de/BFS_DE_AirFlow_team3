WITH cp as (
    SELECT * FROM {{ref('stg_company_profile')}}
),

dim_symbol as (
    SELECT * FROM {{ref('dim_symbols_3')}}
),
dim_cp as (
    SELECT * FROM {{ref('dim_company_profile_3')}}
),
dim_date as (
    select * from {{ ref('dim_date_3') }}
    where is_trading_day = true
),
stock_history as (
    select * from {{ ref('stg_stock_history') }}
),

joined as (
    SELECT ds.symbol, dc.company_id, dd.date_key, 
    sh.DATE, sh.OPEN, sh.CLOSE, sh.HIGH, sh.LOW, sh.VOLUME, sh.ADJCLOSE,
    cp.BETA, cp.VOLAVG as avg_volume, cp.MKTCAP as market_cap, cp.LASTDIV, cp.RANGE, cp.CHANGES as price_change, cp.DCFDIFF, cp.DCF,
    sh.CLOSE - sh.OPEN as daily_change, COALESCE((sh.CLOSE - sh.OPEN)/ NULLIF(sh.OPEN, 0), 0) as daily_change_pct,
ROUND(AVG(sh.ADJCLOSE) OVER (PARTITION BY sh.SYMBOL ORDER BY sh.DATE ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 2) as moving_avg_7_days, 
ROUND(AVG(sh.ADJCLOSE) OVER (PARTITION BY sh.SYMBOL ORDER BY sh.DATE ROWS BETWEEN 29 PRECEDING AND CURRENT ROW), 2) as moving_avg_30_days,
ROUND(AVG(sh.ADJCLOSE) OVER (PARTITION BY sh.SYMBOL ORDER BY sh.DATE ROWS BETWEEN 89 PRECEDING AND CURRENT ROW), 2) as moving_avg_90_days
    FROM stock_history sh 
    JOIN dim_symbol ds ON sh.symbol = ds.symbol
    JOIN dim_cp dc ON dc.symbol = ds.symbol AND dc.is_current = TRUE
    JOIN dim_date dd ON sh.date = dd.full_date
    JOIN cp ON sh.symbol = cp.symbol
)

SELECT DISTINCT
    {{ dbt_utils.generate_surrogate_key(['SYMBOL', 'DATE_KEY', 'COMPANY_ID']) }} as FACT_ID,
    joined.*
FROM 
joined
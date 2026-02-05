SELECT 
    {{ dbt_utils.generate_surrogate_key(['cp.SYMBOL', 'DATE', 'COMPANY_ID']) }} as FACT_ID,
cp_dim.COMPANY_ID, cp.SYMBOL, 
stock_history.DATE, stock_history.OPEN, stock_history.CLOSE, 
stock_history.HIGH, stock_history.LOW, stock_history.VOLUME, stock_history.ADJCLOSE,
cp.BETA, cp.VOLAVG as avg_volume, cp.MKTCAP as market_cap, cp.LASTDIV, cp.RANGE, 
cp.CHANGES as price_change, cp.DCFDIFF, cp.DCF,
stock_history.CLOSE - stock_history.OPEN as daily_change, COALESCE((stock_history.CLOSE - stock_history.OPEN)/ NULLIF(stock_history.OPEN, 0), 0) as daily_change_pct,
AVG(stock_history.ADJCLOSE) OVER (PARTITION BY stock_history.SYMBOL ORDER BY stock_history.DATE ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) as moving_avg_7_days, 
AVG(stock_history.ADJCLOSE) OVER (PARTITION BY stock_history.SYMBOL ORDER BY stock_history.DATE ROWS BETWEEN 29 PRECEDING AND CURRENT ROW) as moving_avg_30_days,
AVG(stock_history.ADJCLOSE) OVER (PARTITION BY stock_history.SYMBOL ORDER BY stock_history.DATE ROWS BETWEEN 89 PRECEDING AND CURRENT ROW) as moving_avg_90_days
FROM 
{{ ref('stg_stock_history')}} as stock_history
JOIN {{ref('stg_company_profile')}} as cp 
ON cp.symbol = stock_history.symbol
JOIN {{ ref('dim_company_profile_3') }} as cp_dim 
ON cp_dim.symbol = stock_history.symbol AND cp_dim.is_current = TRUE


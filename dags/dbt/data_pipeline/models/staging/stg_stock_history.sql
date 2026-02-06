with raw_data as (
  select
    symbol,
    date,
    open,
    high,
    low,
    close,
    volume,
    adjclose
  from {{ source('stock_data', 'stock_history')}} 
)

select
  symbol,
  date,
  max(open)     as open,
  max(high)     as high,
  max(low)      as low,
  max(close)    as close,
  max(adjclose) as adjclose,
  max(volume)   as volume
from raw_data
group by symbol,date
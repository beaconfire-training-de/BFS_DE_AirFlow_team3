SELECT 
symbol,name,exchange
FROM {{ ref('stg_symbols')}} as symbols
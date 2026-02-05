MERGE INTO AIRFLOW0105.DEV.DIM_SYMBOL_BRANDON tgt
USING US_STOCK_DAILY.DCCM.Symbols src
ON tgt.symbol = src.symbol

WHEN MATCHED THEN
UPDATE SET
    tgt.name = src.name,
    tgt.exchange = src.exchange

WHEN NOT MATCHED THEN
INSERT (symbol, name, exchange)
VALUES (src.symbol, src.name, src.exchange);

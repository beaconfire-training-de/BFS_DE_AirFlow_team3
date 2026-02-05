MERGE INTO AIRFLOW0105.DEV.FACT_STOCK_HISTORY_BRANDON tgt
USING (
    SELECT
        sh.symbol,
        sh.date,
        sh.symbol AS company_id,

        sh.open,
        sh.high,
        sh.low,
        sh.close,
        sh.volume,
        sh.adjclose,

        sh.close - sh.open AS daily_change,
        (sh.close - sh.open) / NULLIF(sh.open, 0) AS daily_change_pct,

        cp.beta,
        cp.mktcap,
        cp.lastdiv,
        cp.range,
        cp.dcf,
        cp.dcfdiff

    FROM US_STOCK_DAILY.DCCM.Stock_History sh
    JOIN US_STOCK_DAILY.DCCM.Company_Profile cp
        ON sh.symbol = cp.symbol
) src
ON tgt.symbol = src.symbol
AND tgt.date = src.date

WHEN MATCHED THEN
UPDATE SET
    tgt.company_id        = src.company_id,
    tgt.open              = src.open,
    tgt.high              = src.high,
    tgt.low               = src.low,
    tgt.close             = src.close,
    tgt.volume            = src.volume,
    tgt.adjclose          = src.adjclose,
    tgt.daily_change      = src.daily_change,
    tgt.daily_change_pct  = src.daily_change_pct,
    tgt.beta              = src.beta,
    tgt.mktcap            = src.mktcap,
    tgt.lastdiv           = src.lastdiv,
    tgt.range             = src.range,
    tgt.dcf               = src.dcf,
    tgt.dcfdiff           = src.dcfdiff

WHEN NOT MATCHED THEN
INSERT (
    field,
    company_id,
    symbol,
    date,
    open,
    high,
    low,
    close,
    volume,
    adjclose,
    daily_change,
    daily_change_pct,
    beta,
    mktcap,
    lastdiv,
    range,
    dcf,
    dcfdiff
)
VALUES (
    UUID_STRING(),
    src.company_id,
    src.symbol,
    src.date,
    src.open,
    src.high,
    src.low,
    src.close,
    src.volume,
    src.adjclose,
    src.daily_change,
    src.daily_change_pct,
    src.beta,
    src.mktcap,
    src.lastdiv,
    src.range,
    src.dcf,
    src.dcfdiff
);
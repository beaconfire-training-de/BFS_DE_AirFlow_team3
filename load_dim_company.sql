MERGE INTO AIRFLOW0105.DEV.DIM_COMPANY_PROFILE tgt
USING US_STOCK_DAILY.DCCM.Company_Profile src
ON tgt.company_id = src.symbol
AND tgt.is_current = TRUE

WHEN MATCHED AND (
    NVL(tgt.ceo, '') <> NVL(src.ceo, '')
 OR NVL(tgt.sector, '') <> NVL(src.sector, '')
 OR NVL(tgt.industry, '') <> NVL(src.industry, '')
) THEN
UPDATE SET
    tgt.is_current = FALSE,
    tgt.end_date = CURRENT_DATE

WHEN NOT MATCHED THEN
INSERT (
    company_id,
    name,
    industry,
    website,
    description,
    ceo,
    sector,
    is_current,
    effective_date,
    end_date
)
VALUES (
    src.symbol,
    src.companyname,
    src.industry,
    src.website,
    src.description,
    src.ceo,
    src.sector,
    TRUE,
    CURRENT_DATE,
    NULL
);

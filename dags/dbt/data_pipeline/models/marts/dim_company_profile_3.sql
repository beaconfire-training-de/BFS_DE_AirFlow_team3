select
  {{ dbt_utils.generate_surrogate_key(['SYMBOL','dbt_valid_from']) }} as COMPANY_ID,
  SYMBOL,
  COMPANYNAME as COMPANY_NAME,
  INDUSTRY,
  WEBSITE,
  DESCRIPTION,
  CEO,
  SECTOR,

  (dbt_valid_to is null) as is_current,
  dbt_valid_from         as effective_date,
  dbt_valid_to           as end_date

from {{ ref('snap_company_profile') }}

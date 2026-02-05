{% snapshot snap_company_profile %}
{{
  config(
    target_schema = 'DEV',
    unique_key    = 'SYMBOL',
    strategy      = 'check',
    check_cols    = ['COMPANYNAME','INDUSTRY','WEBSITE','DESCRIPTION','CEO','SECTOR']
  )
}}

select
  SYMBOL,
  COMPANYNAME,
  INDUSTRY,
  WEBSITE,
  DESCRIPTION,
  CEO,
  SECTOR
from {{ ref('stg_company_profile') }}

{% endsnapshot %}

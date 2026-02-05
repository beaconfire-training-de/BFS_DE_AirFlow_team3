
{{ 
    config(
        materialized = 'table',
    )
}}

-- double check this later
{% set holidays = [
    '2024-01-01', '2024-01-15', '2024-02-19', '2024-03-29', '2024-05-27',
    '2024-06-19', '2024-07-04', '2024-09-02', '2024-11-28', '2024-12-25',
    '2025-01-01', '2025-01-20', '2025-02-17', '2025-04-18', '2025-05-26',
    '2025-06-19', '2025-07-04', '2025-09-01', '2025-11-27', '2025-12-25',
    '2026-01-01', '2026-01-19', '2026-02-16', '2026-04-03', '2026-05-25',
    '2026-06-19', '2026-07-03', '2026-09-07', '2026-11-26', '2026-12-25'
] %}


with raw_dates as (
    {{ dbt_date.get_date_dimension('1950-01-01', '2050-01-01') }}
)

SELECT
    to_number(to_char(date_day, 'YYYYMMDD')) as DATE_KEY,
    date_day as full_date,
    day_of_week,
    day_of_week_name,
    day_of_month,
    day_of_year,
    week_of_year,
    month_of_year as month_number,
    month_name,
    quarter_of_year as quarter,
    year_number as year,

    case 
        when day_of_week in (1,7) then false
        when date_day in (
            {% for holiday in holidays %}
                cast('{{ holiday }}' as date){% if not loop.last %},{% endif %}
            {% endfor %}
        ) then false
        else true
    end as is_trading_day
FROM raw_dates
ORDER BY DATE_KEY

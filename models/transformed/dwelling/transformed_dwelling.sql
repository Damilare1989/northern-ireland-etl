{{
  config(
    materialized='table',
    sort=['year', 'quarter', 'lgd'],
    dist='even',
    pre_hook="analyze {{ this }}",
    post_hook="analyze {{ this }}"
  )
}}

WITH row_numbered AS (
  SELECT 
    *,
    ROW_NUMBER() OVER (ORDER BY lgd) as rn,
    COUNT(*) OVER () as total_rows
  FROM {{ source('public', 'new_dwelling_completions') }}
),

filtered_data AS (
  SELECT *
  FROM row_numbered
  WHERE rn < total_rows
),

-- Defining quarters using simplified Jinja templating
quarters AS (
  {%- set quarters = [] %}
  {%- for year in range(2005, 2016) %}
    {%- for quarter in range(1, 5) %}
      {%- if not (year == 2015 and quarter > 1) %}
        {%- do quarters.append({'year': year, 'quarter': quarter}) %}
      {%- endif %}
    {%- endfor %}
  {%- endfor %}
  
  {%- for q in quarters %}
    SELECT 
      {{ q['year'] }} as yr,
      {{ q['quarter'] }} as qtr,
      'q{{ q['quarter'] }}_{{ q['year'] }}' as col_name
    {%- if not loop.last %}
      UNION ALL
    {%- endif %}
  {%- endfor %}
)

SELECT 
  f.lgd,
  CAST(q.yr AS VARCHAR) as year,
  CAST(q.qtr AS VARCHAR) as quarter,
  CASE q.col_name
    {%- for q in quarters %}
    WHEN 'q{{ q['quarter'] }}_{{ q['year'] }}' THEN "q{{ q['quarter'] }}_{{ q['year'] }}"
    {%- endfor %}
  END::numeric as new_dwelling_total
FROM filtered_data f
CROSS JOIN quarters q
ORDER BY q.yr, q.qtr, f.lgd
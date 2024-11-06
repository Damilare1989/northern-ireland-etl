-- models/transformed/dwelling/transformed_dwelling.sql

WITH row_numbered AS (
  SELECT 
    *,
    ROW_NUMBER() OVER (ORDER BY lgd) as rn,
    COUNT(*) OVER () as total_rows
  FROM {{ source('public', 'new_dwelling_completions') }}
),
-- Remove the last row
filtered_data AS (
  SELECT *
  FROM row_numbered
  WHERE rn < total_rows  -- This excludes the last row, equivalent to iloc[:-1]
)

-- Unpivot using UNION ALL for quarters from 2015 to 2023
SELECT 
    lgd,
    '2015' as year,
    '1' as quarter,
    "q1 2015"::numeric as new_dwelling_total
FROM filtered_data
UNION ALL
SELECT 
    lgd,
    '2015' as year,
    '2' as quarter,
    "q2 2015"::numeric as new_dwelling_total
FROM filtered_data
UNION ALL
SELECT 
    lgd,
    '2015' as year,
    '3' as quarter,
    "q3 2015"::numeric as new_dwelling_total
FROM filtered_data
UNION ALL
SELECT 
    lgd,
    '2015' as year,
    '4' as quarter,
    "q4 2015"::numeric as new_dwelling_total
FROM filtered_data
UNION ALL
SELECT 
    lgd,
    '2016' as year,
    '1' as quarter,
    "q1 2016"::numeric as new_dwelling_total
FROM filtered_data
UNION ALL
SELECT 
    lgd,
    '2016' as year,
    '2' as quarter,
    "q2 2016"::numeric as new_dwelling_total
FROM filtered_data
UNION ALL
SELECT 
    lgd,
    '2016' as year,
    '3' as quarter,
    "q3 2016"::numeric as new_dwelling_total
FROM filtered_data
UNION ALL
SELECT 
    lgd,
    '2016' as year,
    '4' as quarter,
    "q4 2016"::numeric as new_dwelling_total
FROM filtered_data
UNION ALL
SELECT 
    lgd,
    '2017' as year,
    '1' as quarter,
    "q1 2017"::numeric as new_dwelling_total
FROM filtered_data
UNION ALL
SELECT 
    lgd,
    '2017' as year,
    '2' as quarter,
    "q2 2017"::numeric as new_dwelling_total
FROM filtered_data
UNION ALL
SELECT 
    lgd,
    '2017' as year,
    '3' as quarter,
    "q3 2017"::numeric as new_dwelling_total
FROM filtered_data
UNION ALL
SELECT 
    lgd,
    '2017' as year,
    '4' as quarter,
    "q4 2017"::numeric as new_dwelling_total
FROM filtered_data
UNION ALL
SELECT 
    lgd,
    '2018' as year,
    '1' as quarter,
    "q1 2018"::numeric as new_dwelling_total
FROM filtered_data
UNION ALL
SELECT 
    lgd,
    '2018' as year,
    '2' as quarter,
    "q2 2018"::numeric as new_dwelling_total
FROM filtered_data
UNION ALL
SELECT 
    lgd,
    '2018' as year,
    '3' as quarter,
    "q3 2018"::numeric as new_dwelling_total
FROM filtered_data
UNION ALL
SELECT 
    lgd,
    '2018' as year,
    '4' as quarter,
    "q4 2018"::numeric as new_dwelling_total
FROM filtered_data
UNION ALL
SELECT 
    lgd,
    '2019' as year,
    '1' as quarter,
    "q1 2019"::numeric as new_dwelling_total
FROM filtered_data
UNION ALL
SELECT 
    lgd,
    '2019' as year,
    '2' as quarter,
    "q2 2019"::numeric as new_dwelling_total
FROM filtered_data
UNION ALL
SELECT 
    lgd,
    '2019' as year,
    '3' as quarter,
    "q3 2019"::numeric as new_dwelling_total
FROM filtered_data
UNION ALL
SELECT 
    lgd,
    '2019' as year,
    '4' as quarter,
    "q4 2019"::numeric as new_dwelling_total
FROM filtered_data
UNION ALL
SELECT 
    lgd,
    '2020' as year,
    '1' as quarter,
    "q1 2020"::numeric as new_dwelling_total
FROM filtered_data
UNION ALL
SELECT 
    lgd,
    '2020' as year,
    '2' as quarter,
    "q2 2020"::numeric as new_dwelling_total
FROM filtered_data
UNION ALL
SELECT 
    lgd,
    '2020' as year,
    '3' as quarter,
    "q3 2020"::numeric as new_dwelling_total
FROM filtered_data
UNION ALL
SELECT 
    lgd,
    '2020' as year,
    '4' as quarter,
    "q4 2020"::numeric as new_dwelling_total
FROM filtered_data
UNION ALL
SELECT 
    lgd,
    '2021' as year,
    '1' as quarter,
    "q1 2021"::numeric as new_dwelling_total
FROM filtered_data
UNION ALL
SELECT 
    lgd,
    '2021' as year,
    '2' as quarter,
    "q2 2021"::numeric as new_dwelling_total
FROM filtered_data
UNION ALL
SELECT 
    lgd,
    '2021' as year,
    '3' as quarter,
    "q3 2021"::numeric as new_dwelling_total
FROM filtered_data
UNION ALL
SELECT 
    lgd,
    '2021' as year,
    '4' as quarter,
    "q4 2021"::numeric as new_dwelling_total
FROM filtered_data
UNION ALL
SELECT 
    lgd,
    '2022' as year,
    '1' as quarter,
    "q1 2022"::numeric as new_dwelling_total
FROM filtered_data
UNION ALL
SELECT 
    lgd,
    '2022' as year,
    '2' as quarter,
    "q2 2022"::numeric as new_dwelling_total
FROM filtered_data
UNION ALL
SELECT 
    lgd,
    '2022' as year,
    '3' as quarter,
    "q3 2022"::numeric as new_dwelling_total
FROM filtered_data
UNION ALL
SELECT 
    lgd,
    '2022' as year,
    '4' as quarter,
    "q4 2022"::numeric as new_dwelling_total
FROM filtered_data
UNION ALL
SELECT 
    lgd,
    '2023' as year,
    '1' as quarter,
    "q1 2023"::numeric as new_dwelling_total
FROM filtered_data
UNION ALL
SELECT 
    lgd,
    '2023' as year,
    '2' as quarter,
    "q2 2023"::numeric as new_dwelling_total
FROM filtered_data
UNION ALL
SELECT 
    lgd,
    '2023' as year,
    '3' as quarter,
    "q3 2023"::numeric as new_dwelling_total
FROM filtered_data
ORDER BY year, quarter, lgd
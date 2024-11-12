-- test_vw_crime_dwelling_unique_districts.sql

WITH district_counts AS (
    SELECT 
        policing_district,
        year,
        quarter,
        COUNT(*) as count
    FROM {{ ref('vw_crime_dwelling') }}
    GROUP BY 
        policing_district,
        year,
        quarter
    HAVING COUNT(*) > 1
)

SELECT *
FROM district_counts
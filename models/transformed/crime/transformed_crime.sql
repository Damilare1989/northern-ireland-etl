WITH cleaned_data AS (
    -- Removing leading/trailing whitespaces and filter out unwanted rows
    SELECT 
        TRIM(policing_district) as policing_district,
        crime_type,
        calendar_year,
        month,
        CAST(count AS FLOAT) as count 
    FROM {{ source('public', 'police_recorded_crime') }}
    WHERE crime_type != 'Total police recorded crime'
    AND TRIM(policing_district) != 'Northern Ireland'
),

quarter_mapping AS (
    -- Adding quarter mapping
    SELECT 
        *,
        CASE 
            WHEN month IN ('Jan', 'Feb', 'Mar') THEN 1
            WHEN month IN ('Apr', 'May', 'Jun') THEN 2
            WHEN month IN ('Jul', 'Aug', 'Sep') THEN 3
            WHEN month IN ('Oct', 'Nov', 'Dec') THEN 4
        END as quarter
    FROM cleaned_data
    WHERE (calendar_year >= 2005 AND calendar_year <= 2014)
    OR (calendar_year = 2015 AND TRIM(month) IN ('Jan', 'Feb', 'Mar'))
)

-- Final aggregation
SELECT 
    calendar_year,
    quarter,
    policing_district,
    crime_type,
    ROUND(SUM(COALESCE(count, 0)))::INTEGER as count
FROM quarter_mapping
WHERE quarter IS NOT NULL
GROUP BY 
    calendar_year,
    quarter,
    policing_district,
    crime_type
ORDER BY 
    calendar_year,
    quarter,
    policing_district,
    crime_type
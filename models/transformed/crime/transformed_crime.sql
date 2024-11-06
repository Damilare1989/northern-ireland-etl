WITH cleaned_data AS (
    -- Remove leading/trailing whitespaces and filter out unwanted rows
    SELECT 
        TRIM(policing_district) as policing_district,
        crime_type,
        calendar_year,
        month,
        -- Convert count to numeric, replacing invalid values with NULL
        TRY_CAST(count AS INTEGER) as count
    FROM {{ source('public', 'police_recorded_crime') }}
    WHERE crime_type != 'Total police recorded crime'
    AND policing_district != 'Northern Ireland'
),

quarter_mapping AS (
    -- Add quarter mapping
    SELECT 
        *,
        CASE 
            WHEN month IN ('Jan', 'Feb', 'Mar') THEN 1
            WHEN month IN ('Apr', 'May', 'Jun') THEN 2
            WHEN month IN ('Jul', 'Aug', 'Sep') THEN 3
            WHEN month IN ('Oct', 'Nov', 'Dec') THEN 4
        END as quarter
    FROM cleaned_data
    WHERE (calendar_year BETWEEN 2005 AND 2014)
    OR (calendar_year = 2015 AND month IN ('Jan', 'Feb', 'Mar'))
)

-- Final aggregation
SELECT 
    calendar_year,
    quarter,
    policing_district,
    crime_type,
    ROUND(SUM(count))::INTEGER as count
FROM quarter_mapping
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
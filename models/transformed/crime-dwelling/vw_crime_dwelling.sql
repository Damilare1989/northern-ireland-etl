{{ config(
    materialized = 'view'
) }}

-- Step 1: Aggregate crime data by policing district, year, and quarter
WITH aggregated_crime AS (
    SELECT
        policing_district,
        calendar_year,
        quarter,
        SUM(count) as count
    FROM {{ ref('transformed_crime') }}
    GROUP BY 1, 2, 3
),

-- Step 2: Filter the crime data for desired policing districts
filtered_crime AS (
    SELECT *
    FROM aggregated_crime
    WHERE policing_district IN (
        'Ards & North Down',
        'Armagh City Banbridge & Craigavon',
        'Derry City & Strabane',
        'Newry and Mourne & Down',
        'Antrim & Newtownabbey',
        'Lisburn & Castlereagh City',
        'Belfast City',
        'Fermanagh & Omagh'
    )
),

-- Step 3: Map lgds to grouping in dwelling data
dwelling_with_groups AS (
    SELECT 
        d.*,
        CASE d.lgd
            WHEN 'Ards' THEN 'Ards & North Down'
            WHEN 'North Down' THEN 'Ards & North Down'
            WHEN 'Armagh' THEN 'Armagh City Banbridge & Craigavon'
            WHEN 'Craigavon' THEN 'Armagh City Banbridge & Craigavon'
            WHEN 'Banbridge' THEN 'Armagh City Banbridge & Craigavon'
            WHEN 'Derry' THEN 'Derry City & Strabane'
            WHEN 'Strabane' THEN 'Derry City & Strabane'
            WHEN 'Down' THEN 'Newry and Mourne & Down'
            WHEN 'Newry and Mourne' THEN 'Newry and Mourne & Down'
            WHEN 'Antrim' THEN 'Antrim & Newtownabbey'
            WHEN 'Newtownabbey' THEN 'Antrim & Newtownabbey'
            WHEN 'Lisburn' THEN 'Lisburn & Castlereagh City'
            WHEN 'Castlereagh' THEN 'Lisburn & Castlereagh City'
            WHEN 'Fermanagh' THEN 'Fermanagh & Omagh'
            WHEN 'Omagh' THEN 'Fermanagh & Omagh'
            WHEN 'Belfast' THEN 'Belfast City'
        END as lgd_group
    FROM {{ ref('transformed_dwelling')}} d
),

-- Step 4: Aggregate dwelling data by lgd_group, year, and quarter
aggregated_dwelling AS (
    SELECT
        lgd_group,
        year,
        quarter,
        SUM(new_dwelling_total) as new_dwelling_total
    FROM dwelling_with_groups
    WHERE lgd_group IS NOT NULL
    GROUP BY 1, 2, 3
),

-- Step 5: Generate unique lgd_group_code for each lgd_group
group_codes AS (
    SELECT
        lgd_group,
        ROW_NUMBER() OVER (ORDER BY lgd_group) + 99 as lgd_group_code
    FROM (
        SELECT DISTINCT lgd_group 
        FROM aggregated_dwelling
        ORDER BY lgd_group
    ) 
),

-- Step 6: Merge aggregated dwelling and crime data based on lgd_group, year, and quarter
final_result AS (
    SELECT
        c.policing_district,
        d.year,
        d.quarter,
        d.new_dwelling_total,
        c.count as crime_total
    FROM aggregated_dwelling d
    JOIN group_codes g ON d.lgd_group = g.lgd_group
    JOIN filtered_crime c ON 
        g.lgd_group = c.policing_district AND
        d.year = c.calendar_year AND
        d.quarter = c.quarter
)

-- Step 7: Select and order final result columns 
SELECT
    policing_district,
    year,
    quarter,
    new_dwelling_total,
    crime_total
FROM final_result
ORDER BY 
    policing_district,
    year,
    quarter

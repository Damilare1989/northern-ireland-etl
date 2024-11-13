# Crime and Dwelling Data Transformation Project

## Overview
This dbt project transforms and combines police recorded crime data with new dwelling completions data across Northern Ireland districts. The project aggregates data by policing district, year, and quarter to enable analysis of potential correlations between crime rates and new housing developments.

## Data Sources
- `police_recorded_crime`: Raw crime statistics data
- `new_dwelling_completions`: Raw housing completion data

## Models

### 1. transformed_crime
- Cleans and transforms raw crime data
- Removes unwanted rows (e.g., 'Total police recorded crime')
- Maps months to quarters
- Aggregates crime counts by district, year, quarter, and crime type
- Date range: 2005-2015 (Q1)

### 2. transformed_dwelling
- Processes new dwelling completion data
- Handles district mapping and data standardization
- Aggregates dwelling counts by district, year, and quarter
- Date range: 2005-2015 (Q1)

### 3. vw_crime_dwelling
Final view combining crime and dwelling data with the following transformations:
1. Aggregates crime data by policing district
2. Filters for specific policing districts
3. Maps Local Government Districts (LGDs) to consistent groupings
4. Aggregates dwelling data by district groups
5. Generates district codes
6. Merges crime and dwelling data

## Tests
- `test_vw_crime_dwelling_unique_districts.sql`: Ensures no duplicate district entries for any year/quarter combination

## District Mappings
The project handles the following district consolidations:
- Ards & North Down (Ards, North Down)
- Armagh City Banbridge & Craigavon (Armagh, Craigavon, Banbridge)
- Derry City & Strabane (Derry, Strabane)
- Newry and Mourne & Down (Down, Newry and Mourne)
- Antrim & Newtownabbey (Antrim, Newtownabbey)
- Lisburn & Castlereagh City (Lisburn, Castlereagh)
- Belfast City (Belfast)
- Fermanagh & Omagh (Fermanagh, Omagh)

## Configuration
- `transformed_dwelling`: Materialized as table with sort and distribution keys
- `vw_crime_dwelling`: Materialized as view
- Includes pre and post-hook analysis steps

## Output Schema
Final view (`vw_crime_dwelling`) columns:
- `policing_district`: District name
- `year`: Integer (2005-2015)
- `quarter`: Integer (1-4)
- `new_dwelling_total`: Number of new dwellings completed
- `crime_total`: Total number of recorded crimes

## Usage
To run the full transformation:
```bash
dbt run
```

To run tests:
```bash
dbt test
```

## Dependencies
- dbt Core
- Warehouse supporting window functions and CASE statements
- Source tables must be available in the 'public' schema

## Contributing
When adding new transformations:
1. Follow existing naming conventions
2. Add appropriate tests
3. Update district mappings if new districts are added
4. Document any schema changes

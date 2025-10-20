-- Materializes a persistent table with the ingestion timestamp and cleaned crime data.
{{ config(
    materialized='table'
) }}

SELECT 
    TIMESTAMP("{{ run_started_at }}") AS dts_ingestion_ts,
    
    -- Primary identifiers
    SAFE_CAST(id AS INT64) AS id,
    case_number,
    
    -- Date fields - convert to proper timestamps
    SAFE.PARSE_TIMESTAMP('%m/%d/%Y %I:%M:%S %p', incident_date) AS incident_date,
    SAFE.PARSE_TIMESTAMP('%m/%d/%Y %I:%M:%S %p', updated_on) AS updated_on,
    SAFE_CAST(incident_year AS INT64) AS incident_year,
    
    -- Location information
    incident_block,
    location_description,
    incident_location,
    
    -- Crime classification
    iucr,
    primary_type,
    secondary_description,
    fbi_code,
    
    -- Boolean flags - convert to proper booleans
    CASE 
        WHEN LOWER(TRIM(arrest)) = 'true' THEN TRUE
        WHEN LOWER(TRIM(arrest)) = 'false' THEN FALSE
        ELSE NULL
    END AS arrest,
    
    CASE 
        WHEN LOWER(TRIM(domestic)) = 'true' THEN TRUE
        WHEN LOWER(TRIM(domestic)) = 'false' THEN FALSE
        ELSE NULL
    END AS domestic,
    
    -- Geographic identifiers
    SAFE_CAST(beat AS INT64) AS beat,
    SAFE_CAST(district AS INT64) AS district,
    SAFE_CAST(ward AS INT64) AS ward,
    SAFE_CAST(community_area AS INT64) AS community_area,
    
    -- Coordinates - convert to proper numeric types
    SAFE_CAST(x_coordinate AS FLOAT64) AS x_coordinate,
    SAFE_CAST(y_coordinate AS FLOAT64) AS y_coordinate,
    SAFE_CAST(latitude AS FLOAT64) AS latitude,
    SAFE_CAST(longitude AS FLOAT64) AS longitude

FROM 
    `{{ target.project }}.dts_01.ext_crimes`
WHERE 
    -- Filter out any completely empty rows
    id IS NOT NULL 
    AND TRIM(id) != ''
    AND case_number IS NOT NULL 
    AND TRIM(case_number) != ''

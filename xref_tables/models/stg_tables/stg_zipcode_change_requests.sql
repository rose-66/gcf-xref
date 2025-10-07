-- Materializes a persistent table with the ingestion timestamp.
{{ config(
    materialized='table', 
    tags=['xref'] 
) }}

SELECT 
    TIMESTAMP("{{ run_started_at }}") AS xref_ingestion_ts,
    t.zip_code,
    t.ae,
    t.state_name,
    t.county,
    t.region,
    t.provider_count,
    t.sales_director,
    
    -- Conversion to BOOL
    (CASE WHEN t.added_to_master IS NOT NULL AND TRIM(t.added_to_master) != '' THEN TRUE ELSE FALSE END) AS is_master_added,
    (CASE WHEN t.added_to_map IS NOT NULL AND TRIM(t.added_to_map) != '' THEN TRUE ELSE FALSE END) AS is_map_added,
    (CASE WHEN t.added_to_baseline IS NOT NULL AND TRIM(t.added_to_baseline) != '' THEN TRUE ELSE FALSE END) AS is_baseline_added,
    (CASE WHEN t.added_to_trilliant IS NOT NULL AND TRIM(t.added_to_trilliant) != '' THEN TRUE ELSE FALSE END) AS is_trilliant_added

FROM 
    `{{ target.project }}.slv_xref.ext_zipcode_change_requests` t
-- Materializes a persistent table with the ingestion timestamp.
{{ config(
    materialized='table', 
    tags=['xref'] 
) }}

SELECT 
    TIMESTAMP("{{ run_started_at }}") AS xref_ingestion_ts,
    zip_code,
    new_territory_name
    -- drops count as it may not be needed
FROM 
    `{{ target.project }}.slv_xref.ext_zip_to_territory` t
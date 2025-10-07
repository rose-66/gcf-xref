-- Materializes a persistent table with the ingestion timestamp.
{{ config(
    materialized='table', 
    tags=['xref'] 
) }}

SELECT 
    TIMESTAMP("{{ run_started_at }}") AS xref_ingestion_ts,
    t.* -- Selects all columns from the external table after the timestamp
FROM 
    `{{ target.project }}.slv_xref.ext_zipcode_territory_ae` t
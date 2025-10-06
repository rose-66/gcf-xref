{{ config(materialized='ephemeral') }}

{% call statement('raw_addcharge_mapping', fetch_result=False) %}
CREATE OR REPLACE EXTERNAL TABLE `{{ target.project }}.slv_xref.ext_addcharge_mapping`
(addcharge_code STRING,
 scan_description STRING,
 separate_scan BOOL)
OPTIONS (
    format = 'CSV',
    uris = ['gs://xref-ext-tables/addcharge_mapping/ingestion_timestamp=20251002_135729/raw_addcharge_mapping.csv'],
    skip_leading_rows = 1,
    field_delimiter = ',',
    allow_quoted_newlines = true,
    allow_jagged_rows = true
);

{% endcall %}
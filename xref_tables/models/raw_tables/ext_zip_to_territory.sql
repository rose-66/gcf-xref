{{ config(materialized='ephemeral') }}

{% call statement('raw_zip_to_territory', fetch_result=False) %}
CREATE OR REPLACE EXTERNAL TABLE `{{ target.project }}.slv_xref.ext_zip_to_territory`
(
    count INT64,
    zip_code STRING,
    new_territory_name STRING
)
OPTIONS (
    format = 'CSV',
    uris = ['gs://xref-ext-tables/commercial_non_pi_quota/ingestion_timestamp=20251002_143623/raw_zip_to_territory.csv'],
    skip_leading_rows = 2,
    field_delimiter = ',',
    allow_quoted_newlines = true,
    allow_jagged_rows = true,
    ignore_unknown_values = true
);

{% endcall %}
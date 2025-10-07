{{ config(materialized='ephemeral') }}

{% call statement('raw_zipcode_territory_il', fetch_result=False) %}
CREATE OR REPLACE EXTERNAL TABLE `{{ target.project }}.slv_xref.ext_zipcode_territory_il`
(
    zip_code STRING,
    ae STRING,
    state_name STRING,
    county STRING,
    region STRING,
    provider_count INT64,
    sales_director STRING
)
OPTIONS (
    format = 'CSV',
    uris = ['gs://xref-ext-tables/zipcode_territory_assignments/ingestion_timestamp=20251002_154727/raw_zipcode_territory_il.csv'],
    skip_leading_rows = 1,
    field_delimiter = ',',
    allow_quoted_newlines = true,
    allow_jagged_rows = true
);

{% endcall %}   
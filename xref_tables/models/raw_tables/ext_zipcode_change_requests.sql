{{ config(materialized='ephemeral') }}

{% call statement('raw_zipcode_change_requests', fetch_result=False) %}
CREATE OR REPLACE EXTERNAL TABLE `{{ target.project }}.slv_xref.ext_zipcode_change_requests`
(
    zip_code STRING,
    ae STRING,
    state_name STRING,
    county STRING,
    region STRING,
    provider_count INT64,
    sales_director STRING,
    added_to_master STRING,
    added_to_map STRING,
    added_to_baseline STRING,
    added_to_trilliant STRING
)
OPTIONS (
    format = 'CSV',
    uris = ['gs://xref-ext-tables/zipcode_territory_assignments/ingestion_timestamp=20251002_154912/raw_zipcode_change_requests.csv'],
    skip_leading_rows = 1,
    field_delimiter = ',',
    allow_quoted_newlines = true,
    allow_jagged_rows = true,
    ignore_unknown_values = true
);

{% endcall %}
{{ config(materialized='ephemeral') }}

{% call statement('raw_2025_tableau_data_fw20', fetch_result=False) %}
CREATE OR REPLACE EXTERNAL TABLE `{{ target.project }}.slv_xref.ext_2025_tableau_data_fw20`
(
    site_name STRING,
    practice STRING,
    requested_date INT64,
    region STRING,
    units FLOAT64,
    modality_group STRING,
    ris STRING,
    same_store BOOL

)
OPTIONS (
    format = 'CSV',
    uris = ['gs://xref-ext-tables/fixed_vs_adg_orders/ingestion_timestamp=20251002_154607/raw_2025_tableau_data_fw20.csv'],
    skip_leading_rows = 1,
    field_delimiter = ',',
    allow_quoted_newlines = true,
    allow_jagged_rows = true
);

{% endcall %}
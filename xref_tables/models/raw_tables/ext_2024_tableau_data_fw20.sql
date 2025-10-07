{{ config(materialized='ephemeral') }}

{% call statement('raw_2024_tableau_data_fw20', fetch_result=False) %}
CREATE OR REPLACE EXTERNAL TABLE `{{ target.project }}.slv_xref.ext_2024_tableau_data_fw20`
(
    practice STRING,
    site_name STRING,
    region STRING,
    units FLOAT64,
    ris STRING,
    requested_date INT64,
    modality STRING,
    same_store BOOL,
    modality_clean STRING

)
OPTIONS (
    format = 'CSV',
    uris = ['gs://xref-ext-tables/fixed_vs_adg_orders/ingestion_timestamp=20251002_154435/raw_2024_tableau_data_fw20.csv'],
    skip_leading_rows = 1,
    field_delimiter = ',',
    allow_quoted_newlines = true,
    allow_jagged_rows = true
);

{% endcall %}
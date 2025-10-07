{{ config(materialized='ephemeral') }}

{% call statement('raw_ytd_orders_summary', fetch_result=False) %}
CREATE OR REPLACE EXTERNAL TABLE `{{ target.project }}.slv_xref.ext_ytd_orders_summary`
(
    region STRING,
    modality_group STRING,
    leader STRING,
    ytd_25_budget FLOAT64,
    ytd_25_actual FLOAT64,
    ytd_25_gap FLOAT64,
    percent_attainment FLOAT64
)
OPTIONS (
    format = 'CSV',
    uris = ['gs://xref-ext-tables/fixed_vs_adg_orders/ingestion_timestamp=20251002_154407/raw_ytd_order_summary.csv'],
    skip_leading_rows = 1,
    field_delimiter = ',',
    allow_quoted_newlines = true,
    allow_jagged_rows = true
);

{% endcall %}
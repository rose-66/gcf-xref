{{ config(materialized='ephemeral') }}

{% call statement('raw_house_acct_weekly_orders', fetch_result=False) %}
CREATE OR REPLACE EXTERNAL TABLE `{{ target.project }}.slv_xref.ext_house_acct_weekly_orders`
(
    modality_group STRING,
    total_weekly_orders INT64,
    fy_25_orders INT64
)
OPTIONS (
    format = 'CSV',
    uris = ['gs://xref-ext-tables/commercial_non_pi_quota/ingestion_timestamp=20251002_144429/raw_house_acct_weekly_orders.csv'],
    skip_leading_rows = 2,
    field_delimiter = ',',
    allow_quoted_newlines = true,
    allow_jagged_rows = true
);

{% endcall %}
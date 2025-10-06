{{ config(materialized='ephemeral') }}

{% call statement('raw_avg_run_rate_24_25', fetch_result=False) %}
CREATE OR REPLACE EXTERNAL TABLE `{{ target.project }}.slv_xref.ext_avg_run_rate_24_25`
(
    modality_group STRING,
    fixed_weekly INT64,
    fixed_monthly INT64,
    adg_weekly INT64,
    adg_monthly INT64,
    total_weekly INT64,
    total_monthly INT64
)
OPTIONS (
    format = 'CSV',
    uris = ['gs://xref-ext-tables/commercial_non_pi_quota/ingestion_timestamp=20251002_144213/raw_avg_exit_run_rate_24_25.csv'],
    skip_leading_rows = 2,
    field_delimiter = ',',
    allow_quoted_newlines = true,
    allow_jagged_rows = true
);

{% endcall %}
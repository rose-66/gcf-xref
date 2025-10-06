{{ config(materialized='ephemeral') }}

{% call statement('raw_sites_to_terr_list', fetch_result=False) %}
CREATE OR REPLACE EXTERNAL TABLE `{{ target.project }}.slv_xref.ext_sites_to_terr_list`
(
    fixed_territory_name STRING,
    site_name STRING
)
OPTIONS (
    format = 'CSV',
    uris = ['gs://xref-ext-tables/commercial_non_pi_quota/ingestion_timestamp=20251002_143734/raw_commercial_non_pi_quota_terr_list.csv'],
    skip_leading_rows = 4,
    field_delimiter = ',',
    allow_quoted_newlines = true,
    allow_jagged_rows = true,
    ignore_unknown_values = true
);

{% endcall %}
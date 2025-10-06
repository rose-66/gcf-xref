{{ config(materialized='ephemeral') }}

{% call statement('raw_commercial_non_pi_quota_terr', fetch_result=False) %}
CREATE OR REPLACE EXTERNAL TABLE `{{ target.project }}.slv_xref.ext_quota_terr`
(new_territory_name STRING,
 region STRING,
 manager STRING,
 ae STRING,
 quota_and_comp_notes STRING)
OPTIONS (
    format = 'CSV',
    uris = ['gs://xref-ext-tables/commercial_non_pi_quota/ingestion_timestamp=20251002_142308/raw_commercial_non_pi_quota_terr.csv'],
    skip_leading_rows = 1,
    field_delimiter = ',',
    allow_quoted_newlines = true,
    allow_jagged_rows = true
);

{% endcall %}
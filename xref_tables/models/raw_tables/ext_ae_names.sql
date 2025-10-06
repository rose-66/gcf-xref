{{ config(materialized='ephemeral') }}

{% call statement('raw_commercial_non_pi_quota_terr_ae_names', fetch_result=False) %}
CREATE OR REPLACE EXTERNAL TABLE `{{ target.project }}.slv_xref.ext_ae_names`
(new_territory_name STRING,
 region STRING,
 manager STRING,
 vp STRING,
 ae STRING)
OPTIONS (
    format = 'CSV',
    uris = ['gs://xref-ext-tables/commercial_non_pi_quota/ingestion_timestamp=20251003_154039/raw_commercial_non_pi_quota_terr_ae_names.csv'],
    skip_leading_rows = 1,
    field_delimiter = ',',
    allow_quoted_newlines = true,
    allow_jagged_rows = true
);

{% endcall %}
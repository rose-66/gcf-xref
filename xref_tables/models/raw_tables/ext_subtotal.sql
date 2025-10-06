{{ config(materialized='ephemeral') }}

{% call statement('raw_quota_by_month_subtotal', fetch_result=False) %}
CREATE OR REPLACE EXTERNAL TABLE `{{ target.project }}.slv_xref.ext_subtotal`
(modality_group STRING,
 january INT64,
 february INT64,
 march INT64,
 april INT64,
 may INT64,
 june INT64,
 july INT64,
 august INT64,
 september INT64,
 october INT64,
 november INT64,
 december INT64,
 ytd_25_quota INT64)
OPTIONS (
    format = 'CSV',
    uris = ['gs://xref-ext-tables/commercial_non_pi_quota/ingestion_timestamp=20251002_143241/raw_quota_by_month_subtotal.csv'],
    skip_leading_rows = 1,
    field_delimiter = ',',
    allow_quoted_newlines = true,
    allow_jagged_rows = true
);

{% endcall %}
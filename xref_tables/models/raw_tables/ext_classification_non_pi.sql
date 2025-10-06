{{ config(materialized='ephemeral') }}

{% call statement('raw_classification_non_pi', fetch_result=False) %}
CREATE OR REPLACE EXTERNAL TABLE `{{ target.project }}.slv_xref.ext_classification_non_pi`
(classification STRING,
 pi_vs_commercial STRING)
OPTIONS (
    format = 'CSV',
    uris = ['gs://xref-ext-tables/akumin_unified_payer_mapping/ingestion_timestamp=20251002_135619/raw_classification_non_pi.csv'],
    skip_leading_rows = 1,
    field_delimiter = ',',
    allow_quoted_newlines = true,
    allow_jagged_rows = true
);

{% endcall %}
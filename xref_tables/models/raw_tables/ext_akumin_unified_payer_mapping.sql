{{ config(materialized='ephemeral') }}

{% call statement('raw_akumin_unified_payer_mapping', fetch_result=False) %}
CREATE OR REPLACE EXTERNAL TABLE `{{ target.project }}.slv_xref.ext_akumin_unified_payer_mapping`
(carrier_code STRING,
 source STRING,
 payer_description STRING,
 original_payer_name STRING,
 state_name STRING,
 matched_name STRING,
 matched_address STRING,
 bar_id STRING,
 firm_id STRING,
 name_score FLOAT64,
 good_match BOOL,
 classification STRING,
 pi_vs_commercial STRING,
 parent_firm_id STRING,
 firm_name STRING)
OPTIONS (
    format = 'CSV',
    uris = ['gs://xref-ext-tables/akumin_unified_payer_mapping/ingestion_timestamp=20251003_162324/raw_akumin_unified_payer_mapping.csv'],
    skip_leading_rows = 1,
    field_delimiter = ',',
    allow_quoted_newlines = true,
    allow_jagged_rows = true
);

{% endcall %}
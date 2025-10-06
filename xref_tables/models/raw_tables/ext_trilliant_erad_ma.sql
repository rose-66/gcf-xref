{{ config(materialized='ephemeral') }}

{% call statement('raw_trilliant_erad_ma', fetch_result=False) %}
CREATE OR REPLACE EXTERNAL TABLE `{{ target.project }}.slv_xref.ext_trilliant_erad_ma`
(
    provider_id STRING,
    provider_npi STRING,
    active_provider BOOL,
    provider_last_name STRING,
    provider_first_name STRING,
    provider_middle_name STRING,
    provider_suffix STRING,
    provider_affiliated_practice_1_zip_code STRING,
    provider_affiliated_practice_2_zip_code STRING,
    provider_affiliated_practice_3_zip_code STRING,
    provider_affiliated_practice_4_zip_code STRING,
    provider_affiliated_practice_5_zip_code STRING
)
OPTIONS (
    format = 'CSV',
    uris = ['gs://xref-ext-tables/trilliant_data/ingestion_timestamp=20251002_153340/raw_trilliant_erad_ma.csv'],
    skip_leading_rows = 1,
    field_delimiter = ',',
    allow_quoted_newlines = true,
    allow_jagged_rows = true
);

{% endcall %}
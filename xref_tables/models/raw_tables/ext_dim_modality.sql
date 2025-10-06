{{ config(materialized='ephemeral') }}

{% call statement('raw_dim_modality', fetch_result=False) %}
CREATE OR REPLACE EXTERNAL TABLE `{{ target.project }}.slv_xref.ext_dim_modality`
(
    modality_id STRING,
    modality_code STRING,
    an_modality_id STRING,
    modality_name STRING,
    sf_modality_name STRING,
    is_imaging BOOL,
    reporting_modality_type STRING,
    is_active BOOL,
    z_insert_date TIMESTAMP,
    z_last_mod_date TIMESTAMP,
    z_last_mod_by STRING,
    point_factor FLOAT64,
    department_id STRING,
    survey_modality_name STRING
)
OPTIONS (
    format = 'CSV',
    uris = ['gs://xref-ext-tables/dim_data/ingestion_timestamp=20251002_153515/raw_dim_modality.csv'],
    skip_leading_rows = 1,
    field_delimiter = ',',
    allow_quoted_newlines = true,
    allow_jagged_rows = true,
    null_marker = 'NULL'
);

{% endcall %}
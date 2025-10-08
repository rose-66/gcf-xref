-- change data type later to timestamp for date columns
{{ config(materialized='ephemeral') }}

{% call statement('raw_crimes', fetch_result=False) %}
CREATE OR REPLACE EXTERNAL TABLE `{{ target.project }}.dts_01.ext_crimes`
(
    id STRING,
    case_number STRING,
    incident_date STRING,
    incident_block STRING,
    iucr STRING,
    primary_type STRING,
    secondary_description STRING,
    location_description STRING,
    arrest STRING,
    domestic STRING,
    beat STRING,
    district STRING,
    ward STRING,
    community_area STRING,
    fbi_code STRING,
    x_coordinate STRING,
    y_coordinate STRING,
    incident_year STRING,
    updated_on STRING,
    latitude STRING,
    longitude STRING,
    incident_location STRING,
)
OPTIONS (
    format = 'CSV',
    uris = ['gs://dataset-transfer-01/Crimes_-_2001_to_Present_20251008.csv'],
    skip_leading_rows = 1,
    field_delimiter = ',',
    allow_quoted_newlines = true,
    allow_jagged_rows = true
);

{% endcall %}
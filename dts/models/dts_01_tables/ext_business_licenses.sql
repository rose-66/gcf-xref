-- change data type later to timestamp for date columns
{{ config(materialized='ephemeral') }}

{% call statement('raw_business_licenses', fetch_result=False) %}
CREATE OR REPLACE EXTERNAL TABLE `{{ target.project }}.dts_01.ext_business_licenses`
(
    id STRING,
    license_id STRING,
    account_number INT64,
    site_number INT64,
    legal_name STRING,
    doing_business_as_name STRING,
    business_address STRING,
    city STRING,
    business_state STRING,
    zip_code STRING,
    ward INT64,
    precinct STRING,
    ward_precinct STRING,
    police_district INT64,
    community_area INT64,
    community_area_name STRING,
    neighborhood STRING,
    license_code STRING,
    license_description STRING,
    business_activity_id STRING,
    business_activity STRING,
    license_number STRING,
    application_type STRING,
    application_created_date STRING,
    application_requirements_complete STRING,
    payment_date STRING,
    conditional_approval STRING,
    license_term_start_date STRING,
    license_term_expiration_date STRING,
    license_approved_for_issuance STRING,
    date_issued STRING,
    license_status STRING,
    license_status_change_date STRING,
    ssa STRING,
    latitude STRING,
    longitude STRING,
    location_description STRING
)
OPTIONS (
    format = 'CSV',
    uris = ['gs://dataset-transfer-01/Business_Licenses_20251008.csv'],
    skip_leading_rows = 1,
    field_delimiter = ',',
    allow_quoted_newlines = true,
    allow_jagged_rows = true
);

{% endcall %}
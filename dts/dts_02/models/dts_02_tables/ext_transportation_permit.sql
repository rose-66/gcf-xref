-- change data type later to timestamp for date columns
{{ config(materialized='ephemeral') }}

{% call statement('raw_transportation_permit', fetch_result=False) %}
CREATE OR REPLACE EXTERNAL TABLE `{{ target.project }}.dts_02.ext_transportation_permit`
(
    unique_key STRING,
    application_number STRING,
    application_type STRING,
    application_description STRING,
    work_type STRING,
    work_type_description STRING,
    application_status STRING,
    current_milestone STRING,
    application_start_date STRING,
    application_end_date STRING,
    application_processed_date STRING,
    application_issued_date STRING,
    application_finalized_date STRING,
    application_expire_date STRING,
    application_name STRING,
    comments STRING,
    total_fees STRING,
    waived_fees STRING,
    primary_contact_last STRING,
    primary_contact_first STRING,
    primary_contact_middle STRING,
    primary_contact_street STRING,
    primary_contact_street_2 STRING,
    primary_contact_city STRING,
    primary_contact_state STRING,
    primary_contact_zip STRING,
    emergency_contact_name STRING,
    last_inspection_number STRING,
    last_inspection_type STRING,
    last_insp_type_descr STRING,
    last_inspection_date STRING,
    last_inspection_result STRING,
    street_number_from STRING,
    street_number_to STRING,
    direction STRING,
    street_name STRING,
    suffix STRING,
    placement STRING,
    street_closure STRING,
    detail STRING,
    parking_meter_posting_or_bagging STRING,
    ward STRING,
    x_coordinate STRING,
    y_coordinate STRING,
    latitude STRING,
    longitude STRING,
    start_location STRING
)
OPTIONS (
    format = 'CSV',
    uris = ['gs://dts-02/Transportation_Department_Permits_20251008.csv'],
    skip_leading_rows = 1,
    field_delimiter = ',',
    allow_quoted_newlines = true,
    allow_jagged_rows = true,
    null_marker = 'NULL'
);

{% endcall %}
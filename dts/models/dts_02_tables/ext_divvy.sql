-- change data type later to timestamp for date columns
{{ config(materialized='ephemeral') }}

{% call statement('raw_divvy', fetch_result=False) %}
CREATE OR REPLACE EXTERNAL TABLE `{{ target.project }}.dts_02.ext_divvy`
(
    trip_id STRING,
    start_time STRING,
    stop_time STRING,
    bike_id STRING,
    trip_duration STRING,
    from_station_id STRING,
    from_station_name STRING,
    to_station_id STRING,
    to_station_name STRING,
    user_type STRING,
    gender STRING,
    birth_year STRING,
    from_latitude STRING,
    from_longitude STRING,
    from_location STRING,
    to_latitude STRING,
    to_longitude STRING,
    to_location STRING
)
OPTIONS (
    format = 'CSV',
    uris = ['gs://dts-02/Divvy_Trips_20251008.csv'],
    skip_leading_rows = 1,
    field_delimiter = ',',
    allow_quoted_newlines = true,
    allow_jagged_rows = true
);

{% endcall %}
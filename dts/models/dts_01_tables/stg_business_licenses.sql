-- models/dts_01_tables/stg_business_licenses.sql

{{ config(
    materialized='table'
) }}

SELECT
    CAST(t.id AS STRING) AS id,
    CAST(t.license_id AS STRING) AS license_id,
    CAST(t.account_number AS INT64) AS account_number,
    CAST(t.site_number AS INT64) AS site_number,
    t.legal_name,
    t.doing_business_as_name,
    t.business_address,
    t.city,
    t.business_state AS state,
    t.zip_code,
    CAST(t.ward AS INT64) AS ward,
    t.precinct,
    t.ward_precinct,
    CAST(t.police_district AS INT64) AS police_district,
    CAST(t.community_area AS INT64) AS community_area,
    t.community_area_name,
    t.neighborhood,
    t.license_code,
    t.license_description,
    t.business_activity_id,
    t.business_activity,
    t.license_number,
    t.application_type,
    SAFE_CAST(t.application_created_date AS TIMESTAMP) AS application_created_date,
    t.application_requirements_complete,
    SAFE_CAST(t.payment_date AS TIMESTAMP) AS payment_date,
    t.conditional_approval,
    SAFE_CAST(t.license_term_start_date AS DATE) AS license_term_start_date,
    SAFE_CAST(t.license_term_expiration_date AS DATE) AS license_term_expiration_date,
    t.license_approved_for_issuance,
    SAFE_CAST(t.date_issued AS DATE) AS date_issued,
    t.license_status,
    SAFE_CAST(t.license_status_change_date AS TIMESTAMP) AS license_status_change_date,
    t.ssa,
    SAFE_CAST(t.latitude AS FLOAT64) AS latitude,
    SAFE_CAST(t.longitude AS FLOAT64) AS longitude,
    t.location_description
FROM `{{ target.project }}`.dts_01.ext_business_licenses AS t



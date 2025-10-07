{{ config(materialized='ephemeral') }}

{% call statement('raw_npi_organization', fetch_result=False) %}
CREATE OR REPLACE EXTERNAL TABLE `{{ target.project }}.slv_xref.ext_npi_organization`
(
 referring_provider_npi STRING,
 referring_provider_name STRING,
 referring_provider_specialty STRING,
 referring_provider_primary_organization_parent_name STRING,
 referring_provider_primary_organization_name STRING,
 referring_provider_primary_organization_type STRING,
 provider_address STRING,
 provider_city STRING,
 provider_zip_code STRING,
 provider_state STRING
 )
OPTIONS (
    format = 'CSV',
    uris = ['gs://xref-ext-tables/npi_organization/ingestion_timestamp=20251002_153649/raw_npi_to_organization.csv'],
    skip_leading_rows = 1,
    field_delimiter = ',',
    allow_quoted_newlines = true,
    allow_jagged_rows = true
);

{% endcall %}
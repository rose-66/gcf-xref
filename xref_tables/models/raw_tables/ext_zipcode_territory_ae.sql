{{ config(materialized='ephemeral') }}

{% call statement('raw_zipcode_territory_ae', fetch_result=False) %}
CREATE OR REPLACE EXTERNAL TABLE `{{ target.project }}.slv_xref.ext_zipcode_territory_ae`
(
    regional_director STRING,
    regional_director_contact_number STRING,
    ae STRING,
    ae_contact_number STRING,
    primary_site STRING,
    site_manager STRING,
    site_manager_contact_number STRING
)
OPTIONS (
    format = 'CSV',
    uris = ['gs://xref-ext-tables/zipcode_territory_assignments/ingestion_timestamp=20251002_154726/raw_zipcode_territory_ae.csv'],
    skip_leading_rows = 1,
    field_delimiter = ',',
    allow_quoted_newlines = true,
    allow_jagged_rows = true,
    ignore_unknown_values = true
);

{% endcall %}
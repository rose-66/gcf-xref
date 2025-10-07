{{ config(materialized='ephemeral') }}

{% call statement('raw_phelix_procedure', fetch_result=False) %}
CREATE OR REPLACE EXTERNAL TABLE `{{ target.project }}.slv_xref.ext_phelix_procedure`
(
    reason STRING,
    scan_unit INT64,
    note STRING
 )
OPTIONS (
    format = 'CSV',
    uris = ['gs://xref-ext-tables/phelix_procedure/ingestion_timestamp=20251002_153815/raw_phelix_procedure.csv'],
    skip_leading_rows = 1,
    field_delimiter = ',',
    allow_quoted_newlines = true,
    allow_jagged_rows = true,
    ignore_unknown_values = true
);

{% endcall %}
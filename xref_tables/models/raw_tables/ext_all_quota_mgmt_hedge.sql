{{ config(materialized='ephemeral') }}

{% call statement('raw_all_quota_mgmt_hedge', fetch_result=False) %}
CREATE OR REPLACE EXTERNAL TABLE `{{ target.project }}.slv_xref.ext_all_quota_mgmt_hedge`
(
    modality_group STRING,
    q1_jan_21 INT64,
    q1_feb_19 INT64,
    q1_mar_21 INT64,
    q2_apr_22 INT64,
    q2_may_21 INT64,
    q2_jun_20 INT64,
    q2_jul_22 INT64,
    q2_aug_21 INT64,
    q3_sep_21 INT64,
    q4_oct_22 INT64,
    q4_nov_18 INT64,
    q4_dec_22 INT64,
    fy25_target INT64,
    variance_vs_budget FLOAT64,
    target_excl_house_acct INT64,
    variance_vs_target_excl_house_acct FLOAT64
)
OPTIONS (
    format = 'CSV',
    uris = ['gs://xref-ext-tables/commercial_non_pi_quota/ingestion_timestamp=20251002_143833/raw_all_quota_mgmt_hedge.csv'],
    skip_leading_rows = 1,
    field_delimiter = ',',
    allow_quoted_newlines = true,
    allow_jagged_rows = true
);

{% endcall %}
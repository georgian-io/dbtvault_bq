{{
  config(
    materialized='vault_insert_by_period',
    timestamp_field='load_dts',
    period='day',
    start_date='2020-01-01',
    stop_date='2020-01-05'
  )
}}

{{
  dbtvault.hub(
    src_pk='ENTITY_HK',
    src_nk='entity_id',
    src_ldts='load_dts',
    src_source='rec_src',
    source_model='hstg_seed_insert_vault_by_period'
  )
}}
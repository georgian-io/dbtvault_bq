{%- macro bigquery__sat(src_pk, src_hashdiff, src_payload, src_eff, src_ldts, src_source, source_model, out_of_sequence) -%}

{# START > ADDED BY GEORGIAN #}
{%- if out_of_sequence and execute -%}
  {{- exceptions.raise_compiler_error("Out of sequence Sats are not supported by the current version of dbtvault_bq") -}}
{%- endif -%}
{# END   > ADDED BY GEORGIAN #}

{%- set source_cols = dbtvault.expand_column_list(columns=[src_pk, src_hashdiff, src_payload, src_eff, src_ldts, src_source]) -%}

{{ dbtvault.prepend_generated_by() }}

WITH source_data AS (
    SELECT {{ dbtvault.prefix(source_cols, 'a', alias_target='source') }}
    FROM {{ ref(source_model) }} AS a
    {%- set last_cte = "source_data" %}
),

{% if dbtvault.is_any_incremental() -%}
    latest_records_in_sat AS (
        SELECT {{ dbtvault.prefix(source_cols, 'current_records') }}
        FROM {{ this }} AS current_records
        WHERE 1=1
        QUALIFY 1 = RANK() OVER (
            PARTITION BY {{ dbtvault.prefix([src_pk], 'current_records') }}
            ORDER BY {{ dbtvault.prefix([src_ldts], 'current_records') }} DESC
        )
    ),

    max_ldts_per_pk_sat AS (
        SELECT {{ dbtvault.prefix(source_cols, 'latest_records_in_sat') }}
        FROM latest_records_in_sat
        WHERE 1=1
        QUALIFY 
            1 = ROW_NUMBER() OVER (
                PARTITION BY {{ dbtvault.prefix([src_pk], 'latest_records_in_sat') }}
            ) 
    ),

    src_with_stale_dates_removed AS (
        SELECT {{ dbtvault.prefix(source_cols, 'stage')}}
        FROM {{ last_cte }} AS stage 
        JOIN max_ldts_per_pk_sat AS mt
        ON {{ dbtvault.prefix([src_pk], 'mt') }} = {{ dbtvault.prefix([src_pk], 'stage') }}
        WHERE {{ dbtvault.prefix([src_ldts], 'stage') }} >= {{ dbtvault.prefix([src_ldts], 'mt') }}
            OR {{ dbtvault.prefix([src_ldts], 'mt') }} is null
    ),

    {%- set last_cte = "src_with_stale_dates_removed" %}
{% endif -%}

records_to_insert AS (
    SELECT DISTINCT {{ dbtvault.alias_all(source_cols, 'stage') }}
    FROM {{ last_cte }} AS stage
    {%- if dbtvault.is_any_incremental() %}
    LEFT JOIN latest_records_in_sat
    ON {{ dbtvault.prefix([src_pk], 'latest_records_in_sat') }} = {{ dbtvault.prefix([src_pk], 'stage') }} 
        AND {{ dbtvault.prefix([src_hashdiff], 'latest_records_in_sat') }} = {{ dbtvault.prefix([src_hashdiff], 'stage') }}
    WHERE {{ dbtvault.prefix([src_hashdiff], 'latest_records_in_sat') }} IS NULL
    {%- endif %}
)

SELECT * FROM records_to_insert

{%- endmacro -%}
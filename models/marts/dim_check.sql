{{ config(
    materialized = 'table'
) }}
--

WITH source_data AS (

    SELECT
        DISTINCT daily_check
    FROM
        -- {{ ref('correctness_check') }}
        {{ source('epi', 'correctness_check')}}
)
SELECT
    {{ dbt_utils.surrogate_key(['daily_check']) }} AS daily_check_id,
    daily_check,
    {{ dbt_date.now() }} as _write_ts,
    {{ dbt_date.today() }} as _write_date
FROM
    source_data

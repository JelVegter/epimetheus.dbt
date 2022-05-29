{{ config(
    materialized = 'table',
) }}
--

WITH source_data AS (

    SELECT
        DISTINCT daily_check
    FROM
        {{ source('epimetheus', 'correctness_check')}}
)
SELECT
    {{ dbt_utils.surrogate_key(['daily_check']) }} AS daily_check_id,
    daily_check,
    {{ dbt_date.now() }} as _ts
FROM
    source_data

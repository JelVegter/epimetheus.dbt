{{ config(
    materialized = 'table'
) }}
--
WITH source_data AS (

    SELECT
        daily_check, 
        CAST(_ts AS DATE) as concerned_date,
        COUNT(*) AS nr_of_violations
        
    FROM
        {{ source('epimetheus', 'correctness_check')}}
    GROUP BY
        daily_check,
        CAST(_ts AS DATE)
)

SELECT
    {{ dbt_utils.surrogate_key(['daily_check','concerned_date']) }} AS check_id,
    daily_check,
    concerned_date,
    nr_of_violations,
    {{ dbt_date.now() }} as _ts
FROM
    source_data


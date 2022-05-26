{{ config(
    materialized = 'table'
) }}
--
WITH source_data AS (

    SELECT
        * ,
        daily_check,
        CAST(_date AS DATE) as concerned_date,
        COUNT(1) AS nr_of_violations,
        
    FROM
        {{ ref('correctness_check') }}
)

SELECT
    {{ dbt_utils.surrogate_key(['daily_check','concerned_date']) }} AS check_id,
    daily_check,
    concerned_date,
    nr_of_violations,
    {{ dbt_date.now() }} as _write_ts,
    {{ dbt_date.today() }} as _write_date

FROM
    source_data
GROUP BY
    daily_check,
    concerned_date

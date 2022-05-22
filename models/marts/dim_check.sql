{{ config(
    materialized = 'table'
) }}
--
WITH source_data AS (

    SELECT
        DISTINCT daily_check
    FROM
        raw.correctness_check
)
SELECT
    hashbytes(
        'MD5',
        daily_check
    ) AS daily_check_id,
    daily_check
FROM
    source_data

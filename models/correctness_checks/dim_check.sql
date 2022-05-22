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
    daily_check
FROM
    source_data

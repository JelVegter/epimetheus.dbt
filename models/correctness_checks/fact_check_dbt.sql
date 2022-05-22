{{ config(
    materialized = 'table'
) }}
--
WITH source_data AS (

    SELECT
        *
    FROM
        raw.correctness_check
)
SELECT
    daily_check,
    COUNT(1) AS nr_of_violations,
    CAST(
        _date AS DATE
    ) AS concerned_date,
    getdate() AS _ts
FROM
    source_data
GROUP BY
    daily_check,
    CAST(
        _date AS DATE
    )

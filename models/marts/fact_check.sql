{{ config(
    materialized = 'table'
) }}
--
WITH source_data AS (

    SELECT
        *,
        CAST(
            _date AS DATE
        ) AS concerned_date
    FROM
        raw.correctness_check
)
SELECT
    hashbytes(
        'MD5',
        CONCAT(
            daily_check,
            concerned_date
        )
    ) AS daily_check_id,
    daily_check,
    concerned_date,
    COUNT(1) AS nr_of_violations,
    getdate() AS _ts
FROM
    source_data
GROUP BY
    daily_check,
    concerned_date

{{ config(
    materialized = 'table'
) }}
--
WITH source_data AS (

    SELECT
        daily_check, caerus_id, record_id, _date
    FROM
        dbo.fact_check
    WHERE daily_check != 'bets_without_profile'
)
SELECT
    {{ dbt_utils.surrogate_key(['daily_check','caerus_id', '_date']) }} AS id,
    *,
    {{ dbt_date.now() }} as _write_ts,
    {{ dbt_date.today() }} as _write_date
FROM
    source_data

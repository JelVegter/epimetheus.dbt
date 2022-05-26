{{ config(
    materialized = 'table'
) }}
--
WITH source_data AS (

    SELECT
        record, table_name, transaction_dt, extraction_dt, write_dt
    FROM
        dbo.fact_record
)
SELECT
    {{ dbt_utils.surrogate_key(['record','table_name','write_dt']) }} AS id,
    *,
    {{ dbt_date.now() }} as _write_ts,
    {{ dbt_date.today() }} as _write_date
FROM
    source_data

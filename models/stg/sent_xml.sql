{{ config(
    materialized = 'table'
) }}
--
WITH source_data AS (

    SELECT
        xml, table_name, nr_of_records, extraction_dt
    FROM
        dbo.fact_xml
)
SELECT
    {{ dbt_utils.surrogate_key(['xml','extraction_dt']) }} AS id,
    *,
    {{ dbt_date.now() }} as _write_ts,
    {{ dbt_date.today() }} as _write_date
FROM
    source_data

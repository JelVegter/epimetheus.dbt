{{ config(
    materialized = 'table'
) }}
--

WITH source_data AS (

    SELECT
        {% if target.name == 'dev' %}
            TOP 1000
        {% endif %}

        record, 
        table_name, 
        transaction_dt, 
        extraction_dt, 
        write_dt
    FROM
        {{ source('epimetheus', 'sampled_record')}}
    WHERE
        {{ dbt_utils.datediff("write_dt", "getdate()", 'month') }} < 3



)
SELECT
    {{ dbt_utils.surrogate_key(['record','table_name','write_dt']) }} AS record_id,
    record, 
    table_name, 
    transaction_dt, 
    extraction_dt, 
    write_dt,
    {{ dbt_date.now() }} as _ts
FROM
    source_data




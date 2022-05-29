{{ config(
    materialized = 'table'
) }}
--
WITH source_data AS (

    SELECT
        {% if target.name == 'dev' %}
            TOP 1000
        {% endif %}
        xml, table_name, nr_of_records, extraction_dt
    FROM
        {{ source('epimetheus', 'sent_xml')}}
)
SELECT
    {{ dbt_utils.surrogate_key(['xml','table_name','cast(extraction_dt as date)']) }} AS xml_id,
    xml, table_name, nr_of_records, extraction_dt,
    {{ dbt_date.now() }} as _ts
FROM
    source_data


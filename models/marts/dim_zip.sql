{{ config(
    materialized = 'table'
) }}
--
WITH source_data AS (

    SELECT
        {% if target.name == 'dev' %}
            TOP 1000
        {% endif %}
        generation_date,
        datasafe_id,
        blob_name,
        filename
    FROM
        {{ source('epimetheus', 'sent_zip')}}
)
SELECT
    {{ dbt_utils.surrogate_key(['filename','generation_date']) }} AS zip_id,
    datasafe_id,
    blob_name,
    filename,
    {{ dbt_date.now() }} as _ts
FROM
    source_data


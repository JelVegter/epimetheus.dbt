{{ config(
    materialized = 'table'
) }}
--
WITH source_data AS (

    SELECT
        {% if target.name == 'dev' %}
            TOP 1000
        {% endif %}
        date,
        number_of_records,
        datasafe_id,
        earliest_timestamp,
        latest_timestamp,
        generation_date,
        file_size_uncompressed,
        file_size_compressed,
        blob_name,
        filename,
        creation_time_adls
    FROM
        {{ source('epimetheus', 'sent_zip')}}
)
SELECT
    {{ dbt_utils.surrogate_key(['filename','generation_date']) }} AS zip_id,
    date,
    number_of_records,
    datasafe_id,
    earliest_timestamp,
    latest_timestamp,
    generation_date,
    file_size_uncompressed,
    file_size_compressed,
    blob_name,
    filename,
    creation_time_adls,
    {{ dbt_date.now() }} as _ts
FROM
    source_data


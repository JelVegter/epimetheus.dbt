{{ config(
    schema = "epi",
    target_database = "",
    materialized = 'table',
    file_format = 'delta'
) }}
--
WITH source_data AS (

    SELECT
        xml_name
    FROM
        xml_files_bronze
)
SELECT
    *
FROM
    source_data

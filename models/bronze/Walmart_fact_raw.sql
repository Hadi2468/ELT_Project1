/* Walmart_fact_raw: */
{% set columns_table_fact = {
    'STORE': 1,
    'DATE': 2,
    'TEMPERATURE': 3,
    'FUEL_PRICE': 4,
    'MARKDOWN1': 5,
    'MARKDOWN2': 6,
    'MARKDOWN3': 7,
    'MARKDOWN4': 8,
    'MARKDOWN5': 9,
    'CPI': 10,
    'UNEMPLOYMENT': 11,
    'ISHOLIDAY': 12
} %}

{{ config({ "materialized":'table',
            "transient":true,
            "alias":'FACT',
            "pre_hook": macros_copy_csv('FACT', columns_table_fact, '.*fact.*\\.csv'),
            "schema": 'BRONZE' }) }}

WITH transform_3 AS (
    SELECT 
        STORE,
        DATE,
        TEMPERATURE,
        FUEL_PRICE,
        MARKDOWN1,
        MARKDOWN2,
        MARKDOWN3,
        MARKDOWN4,
        MARKDOWN5,
        CPI,
        UNEMPLOYMENT,
        ISHOLIDAY,
        INSERT_DTS AS INSERT_DTS,
        UPDATE_DTS AS UPDATE_DTS,
        SOURCE_FILE_NAME AS SOURCE_FILE_NAME,
        SOURCE_FILE_ROW_NUMBER AS SOURCE_FILE_ROW_NUMBER
    FROM {{source('fact_source','FACT')}}
    )

SELECT *
FROM transform_3

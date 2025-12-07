/* Walmart_stores_raw: */
{% set columns_table_sotres = {
    'STORE': 1,
    'TYPE': 2,
    'SIZE': 3
} %}

{{ config({ "materialized":'table',
            "transient":true,
            "alias":'STORES',
            "pre_hook": macros_copy_csv('STORES', columns_table_sotres, '.*stores.*\\.csv'),
            "schema": 'BRONZE' }) }}

WITH transform_1 AS (
    SELECT 
        STORE,
        TYPE,
        SIZE,
        INSERT_DTS AS INSERT_DTS,
        UPDATE_DTS AS UPDATE_DTS,
        SOURCE_FILE_NAME AS SOURCE_FILE_NAME,
        SOURCE_FILE_ROW_NUMBER AS SOURCE_FILE_ROW_NUMBER
    FROM {{source('stores_source','STORES')}}
    )

SELECT *
FROM transform_1

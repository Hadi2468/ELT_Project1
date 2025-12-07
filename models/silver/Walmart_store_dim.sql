/* Walmart_store_dim: */
{% set columns_table_sotres = {
    'STORE': 1,
    'TYPE': 2,
    'SIZE': 3
} %}

{% set columns_table_department = {
    'STORE': 1,
    'DEPT': 2,
    'DATE': 3,
    'WEEKLY_SALES': 4,
    'ISHOLIDAY': 5
} %}

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
            "alias":'STORES',
            "pre_hook": [
                macros_copy_csv('STORES', columns_table_sotres, '.*stores.*\\.csv'),
                macros_copy_csv('DEPARTMENT', columns_table_department, '.*department.*\\.csv'),
                macros_copy_csv('FACT', columns_table_fact, '.*fact.*\\.csv')
            ],
            "schema": 'SILVER' }) }}

WITH transform_1 AS (
    SELECT 
        STORE,
        TYPE,
        SIZE,
        'EST' AS TIME_ZONE,
        'PRODUCT' AS SOURCE_SYS_NAME,
        'STANDARD' AS INSTNC_ST_NM,
        CURRENT_SESSION() AS PROCESS_ID,
        'TRANSFORM_LOAD' AS PROCESS_NAME,
        INSERT_DTS AS INSERT_DTS,
        UPDATE_DTS AS UPDATE_DTS,
        SOURCE_FILE_NAME AS SOURCE_FILE_NAME,
        SOURCE_FILE_ROW_NUMBER AS SOURCE_FILE_ROW_NUMBER
    FROM {{source('source','STORES')}}
    )

SELECT *
FROM transform

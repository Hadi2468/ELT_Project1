### Walmart_store_dim:
{% set columns_table = {
    'STORE': 1,
    'TYPE': 2,
    'SIZE': 3 } %}

{{ config({ "materialized":'table',
            "transient":true,
            "alias":'STORES',
            "pre_hook": [macros_copy_csv('STORES', columns_table)],
            "schema": 'SILVER' }) }}
 
WITH transform AS (
    SELECT 
        STORE AS STORE,
        TYPE AS TYPE,
        SIZE AS SIZE,
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

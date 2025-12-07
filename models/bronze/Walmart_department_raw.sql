/* Walmart_department_raw.sql */
{% set columns_table_department = {
    'STORE': 1,
    'DEPT': 2,
    'DATE': 3,
    'WEEKLY_SALES': 4,
    'ISHOLIDAY': 5
} %}

{{ config({ "materialized":'table',
            "transient":true,
            "alias":'DEPARTMENT',
            "pre_hook": macros_copy_csv('DEPARTMENT', columns_table_department, '.*department.*\\.csv'),
            "schema": 'BRONZE' }) }}

WITH transform_2 AS (
    SELECT 
        STORE,
        DEPT,
        DATE,
        WEEKLY_SALES,
        ISHOLIDAY,
        INSERT_DTS AS INSERT_DTS,
        UPDATE_DTS AS UPDATE_DTS,
        SOURCE_FILE_NAME AS SOURCE_FILE_NAME,
        SOURCE_FILE_ROW_NUMBER AS SOURCE_FILE_ROW_NUMBER
    FROM {{source('department_source','DEPARTMENT')}}
    )

SELECT *
FROM transform_2

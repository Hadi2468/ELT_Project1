/* Walmart_date_dim.sql */
{% set columns_table_department = {
    'STORE': 1,
    'DEPT': 2,
    'DATE': 3,
    'WEEKLY_SALES': 4,
    'ISHOLIDAY': 5
} %}

{{ config({ "materialized":'table',
            "transient":true,
            "alias":'DATE_DIM',
            "pre_hook": macros_copy_csv('DEPARTMENT', columns_table_department, '.*department.*\\.csv'),
            "schema": 'SILVER' }) }}

WITH transform_5 AS (
    SELECT 
        ROW_NUMBER() OVER(ORDER BY "DATE") AS Date_id,
        "DATE" AS Store_Date,
        ISHOLIDAY AS Isholiday,
        INSERT_DTS AS Insert_date,
        UPDATE_DTS AS Update_date
    FROM {{source('department_source','DEPARTMENT')}} 
    )

SELECT *
FROM transform_5

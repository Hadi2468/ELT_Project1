/* Walmart_store_dim.sql */
{% set columns_table_stOres = {
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

{{ config({ "materialized":'table',
            "transient":true,
            "alias":'STORE_DIM',
            "pre_hook": [
                macros_copy_csv('STORES', columns_table_stOres, '.*stores.*\\.csv'),
                macros_copy_csv('DEPARTMENT', columns_table_department, '.*department.*\\.csv')
            ],
            "schema": 'SILVER' }) }}

WITH transform_4 AS (
    SELECT DISTINCT
        D.STORE AS Store_id,
        D.DEPT AS Dept_id,
        S.TYPE AS Store_type,
        S.SIZE AS Store_size,
        CURRENT_TIMESTAMP() AS Insert_date,
        CURRENT_TIMESTAMP() AS Update_date
    FROM {{source('department_source','DEPARTMENT')}} D
    JOIN {{source('stores_source','STORES')}} S
    ON S.STORE = D.STORE
    )

SELECT *
FROM transform_4

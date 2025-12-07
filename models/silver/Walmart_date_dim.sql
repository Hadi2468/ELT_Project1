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

WITH unique_dates AS (
    SELECT DISTINCT
        "DATE" AS Store_Date,
        ISHOLIDAY AS Isholiday
    FROM {{ source('department_source', 'DEPARTMENT') }}
)

SELECT
    ROW_NUMBER() OVER (ORDER BY Store_Date) AS Date_id,
    Store_Date,
    Isholiday,
    CURRENT_TIMESTAMP() AS Insert_date,
    CURRENT_TIMESTAMP() AS Update_date
FROM unique_dates
ORDER BY Date_id

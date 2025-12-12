/* Walmart_store_dim.sql */
{% set columns_table_stores = {
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

{{ config({
    "materialized": "table",
    "transient": true,
    "alias": "STORE_DIM",
    "pre_hook": [
        macros_copy_csv('STORES', columns_table_stores, '.*stores.*\\.csv'),
        macros_copy_csv('DEPARTMENT', columns_table_department, '.*department.*\\.csv'),
        macros_copy_csv('FACT', columns_table_fact, '.*fact.*\\.csv')
    ],
    "schema": "SILVER"
}) }}

WITH transform_4 AS (
    SELECT DISTINCT
        MD5(CAST(D.STORE AS STRING) || '-' || CAST(D.DEPT AS STRING)) AS UNIQUE_KEY,
        S.STORE AS STORE_ID,
        D.DEPT AS DEPT_ID,
        S.TYPE AS STORE_TYPE,
        S.SIZE AS STORE_SIZE,
        D.INSERT_DTS AS INSERT_DATE,
        D.UPDATE_DTS AS UPDATE_DATE
    FROM {{ source('department_source', 'DEPARTMENT') }} D
    JOIN {{ source('stores_source', 'STORES') }} S
      ON S.STORE = D.STORE
)

SELECT *
FROM transform_4

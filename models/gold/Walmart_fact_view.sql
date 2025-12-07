{{ config({ "materialized":'view',
            "alias":'FACT_VIEW',
            "schema": 'GOLD' }) }}
 
WITH fact_view AS (
    SELECT
        S.STORE_ID,
        S.DEPT_ID,
        S.STORE_SIZE,
        D.WEEKLY_SALES AS STORE_WEEKLY_SALES,
        F.FUEL_PRICE,
        F.TEMPERATURE,
        F.UNEMPLOYMENT AS "UN-EMPLOYMENT",
        F.CPI,
        F.MARKDOWN1,
        F.MARKDOWN2,
        F.MARKDOWN3,
        F.MARKDOWN4,
        F.MARKDOWN5,
        S.DBT_VALID_FROM AS VRSN_START_DATE,
        S.DBT_VALID_TO AS VRSN_UPDATE_DATE,
        S.INSERT_DATE,
        S.UPDATE_DATE
    FROM {{ ref('store_snapshots') }} S
    JOIN {{ source('department_source', 'DEPARTMENT') }} D
    ON S.DEPT_ID = D.DEPT
    AND S.STORE_ID = D.STORE
    JOIN {{ source('fact_source', 'FACT') }} F
    ON S.STORE_ID = F.STORE
)
SELECT * FROM fact_view

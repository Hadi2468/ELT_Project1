/* Walmart_fact_table.sql */
{{ config({ "materialized":'table',
            "alias":'FACT_TABLE',
            "schema": 'GOLD' }) }}
 
WITH fact_table AS (
    SELECT
        SNP.STORE_ID,
        SNP.DEPT_ID,
        D.DATE,
        SNP.STORE_SIZE,
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
        SNP.INSERT_DATE,
        SNP.UPDATE_DATE,
        SNP.DBT_VALID_FROM AS VRSN_START_DATE,
        SNP.DBT_VALID_TO AS VRSN_UPDATE_DATE
    FROM {{ ref('store_snapshots') }} SNP
    JOIN {{ source('department_source', 'DEPARTMENT') }} D
    ON SNP.STORE_ID = D.STORE
    AND SNP.DEPT_ID = D.DEPT
    JOIN {{ source('fact_source', 'FACT') }} F
    ON D.STORE = F.STORE
    AND D.DATE = F.DATE
)
SELECT * FROM fact_table
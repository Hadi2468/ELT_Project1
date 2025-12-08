/* store_snapshot.sql */
{% snapshot store_snapshots %}
{{ config(  target_database='P1_DB',
            target_schema='snapshots',
            unique_key=['STORE_ID', 'DEPT_ID'],
            strategy='check',
            check_cols=['STORE_ID', 'DEPT_ID'], 
        ) }}
select * from {{ source('dim_store_source', 'STORE_DIM') }}
{% endsnapshot %}

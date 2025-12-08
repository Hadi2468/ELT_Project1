/* store_snapshot.sql */
{% snapshot store_snapshots %}
{{ config(  target_database='P1_DB',
            target_schema='snapshots',
            unique_key='UNIQUE_KEY',
            strategy='check',
            check_cols=['STORE_TYPE', 'STORE_SIZE'], 
        ) }}
select * from {{ source('dim_store_source', 'STORE_DIM') }}
{% endsnapshot %}

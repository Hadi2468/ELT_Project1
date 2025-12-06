{% macro macros_copy_csv(table_nm, column_list) %} 

-- Delete existing data
delete from {{ var('rawhist_db') }}.{{ var('wrk_schema') }}.{{ table_nm }};

-- Copy new data
COPY INTO {{ var('rawhist_db') }}.{{ var('wrk_schema') }}.{{ table_nm }}
FROM (
    SELECT
    {%- for col, idx in column_list.items() %}
        ${{ idx }} AS {{ col }}{{ "," }}
    {%- endfor %},
    CURRENT_TIMESTAMP() AS INSERT_DTS,
    CURRENT_TIMESTAMP() AS UPDATE_DTS,
    metadata$filename AS SOURCE_FILE_NAME,
    metadata$file_row_number AS SOURCE_FILE_ROW_NUMBER
    FROM @{{ var('stage_name') }}
)
FILE_FORMAT = {{ var('file_format_csv') }}
PURGE = {{ var('purge_status') }}
FORCE = TRUE
;

{% endmacro %}


{{
    config(tags=["account", "user"])
}}

WITH date_table AS (
    SELECT 
    {% if var('load_date') is none %}
        CONVERT_TIMEZONE ('America/Chicago', getdate())::date as load_date
    {% else %}
        '{{ var("load_date") }}'::date as load_date
    {% endif %}
)

SELECT 
    load_date,
    dateadd(day, -1, load_date)::date as yesterday,
    load_date::timestamp as date_timestamp
FROM date_table

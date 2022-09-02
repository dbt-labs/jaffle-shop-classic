{% macro hash_of_column(column) %}

    SHA2(
        TRIM(
            LOWER(
                CAST({{ column|lower }} AS {{ dbt_utils.type_string() }})
                || '{{ jaffle_shop.get_salt(column|lower) }}'
            )
        ),
        256
    ) AS {{column|lower}}_hash,

{% endmacro %}

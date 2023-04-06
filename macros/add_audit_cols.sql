{% macro add_audit_cols(table_name, modified_by='SYSTEM') %}
SELECT 
  *,
  current_timestamp() AS modified_on,
  '{{ modified_by }}' AS modified_by
FROM {{ table_name }}
{% endmacro %}

 
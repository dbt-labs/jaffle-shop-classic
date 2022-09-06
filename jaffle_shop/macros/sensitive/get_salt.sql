{% macro get_salt(column_name) %}
    {{ return( env_var("SALT") ) }}
{% endmacro %}

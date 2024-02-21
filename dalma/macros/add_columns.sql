{% macro create_columns(columns) %}

{% set relation = 'BLACKROCK.TEST.SCHEMA_DETECTION_STG_TEST' %}
  
	{% for column in columns %}

		{% call statement() %}
		 alter table {{ relation }} add column "{{ column.name }}" {{ column.data_type }};
		{% endcall %}
		
	{% endfor %}
{% endmacro %}

 
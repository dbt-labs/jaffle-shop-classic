{%- macro preview(object_name, order_by=None, preview_limit=5) -%}
select
    *
from {{ object_name }}
{%- if order_by %}
order by {{ order_by }}
{%- endif %}
limit {{ preview_limit }}
;
{%- endmacro -%}

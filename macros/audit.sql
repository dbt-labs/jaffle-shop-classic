{% macro audit(table_name) %}


select *, current_timestamp() as modified_on, 'kishore' as modified_by
     {% if execute %}

        {% if not flags.FULL_REFRESH and config.get('materialized') == "incremental" %}

            {% set source_relation = adapter.get_relation(
                database=target.database,
                schema=this.schema,
                identifier=this.table,
                ) %}      

            {% if source_relation != None %}

                {% set min_created_date %}
                    SELECT LEAST(MIN(dbt_created_at), CURRENT_TIMESTAMP()) AS min_ts 
                    FROM {{ this }}
                {% endset %}

                {% set results = run_query(min_created_date) %}

                , '{{results.columns[0].values()[0]}}'::TIMESTAMP AS dbt_created_at

            {% else %}

                , CURRENT_TIMESTAMP()               AS dbt_created_at

            {% endif %}

        {% else %}

           ,  CURRENT_TIMESTAMP()               AS dbt_created_at

        {% endif %}
    
    {% endif %}

from {{table_name}}
{% endmacro %}

 